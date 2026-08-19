package raft_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/testutil"
	"github.com/sujalbistaa/DistKV/internal/transport"
)

// serviceSnapshotTrigger runs for the lifetime of the test, snapshotting
// node whenever it signals it's accumulated enough entries. This stands in
// for the real application (the milestone 6 KV state machine), which will
// do the same thing with its actual serialized state instead of a
// synthetic marker.
func serviceSnapshotTrigger(t *testing.T, node *raft.Node) {
	t.Helper()
	go func() {
		for index := range node.SnapshotTrigger() {
			_ = node.Snapshot(index, []byte(fmt.Sprintf("snapshot-as-of-%d", index)))
		}
	}()
}

func withSnapshotThreshold(n int) testutil.RaftClusterOpt {
	return func(i int, cfg *raft.Config) {
		cfg.SnapshotThreshold = n
	}
}

// withGenerousTimeouts widens election/heartbeat/RPC timeouts beyond
// RaftCluster's fast, in-memory-transport-tuned defaults. Tests that mix
// real disk fsyncs (DiskStorage, snapshot compaction) with tight proposal
// loops need the headroom, or real I/O latency alone can blow through a
// 30-60ms election timeout and cause spurious leader churn.
func withGenerousTimeouts() testutil.RaftClusterOpt {
	return func(i int, cfg *raft.Config) {
		cfg.ElectionTimeoutMin = 200 * time.Millisecond
		cfg.ElectionTimeoutMax = 400 * time.Millisecond
		cfg.HeartbeatInterval = 40 * time.Millisecond
		cfg.RPCTimeout = 150 * time.Millisecond
	}
}

// proposeWithRetry proposes cmd against whichever node currently reports
// itself as leader, retrying against a fresh one if the current leader
// steps down mid-attempt — exactly what a real client does, and what
// keeps a test from being pinned to one leader reference across a long
// proposal loop where leadership could in principle move on.
func proposeWithRetry(t testing.TB, rc *testutil.RaftCluster, timeout time.Duration, cmd []byte) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range rc.Nodes {
			if _, role := node.State(); role != raft.Leader {
				continue
			}
			if idx, _, ok := node.Propose(cmd); ok {
				return idx
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("could not propose %q within %s: no stable leader", cmd, timeout)
	return 0
}

// TestSnapshotBoundsLogSize: with a low snapshot threshold and every node
// servicing its own SnapshotTrigger, proposing far more entries than the
// threshold must not let any node's in-memory log grow unboundedly.
func TestSnapshotBoundsLogSize(t *testing.T) {
	const threshold = 5
	rc := testutil.NewRaftCluster(t, 5, 400, withSnapshotThreshold(threshold))
	for _, node := range rc.Nodes {
		serviceSnapshotTrigger(t, node)
		drainApplyCh(node)
	}

	rc.AwaitLeader(t, time.Second)
	const nEntries = 60
	var lastIndex uint64
	for i := 0; i < nEntries; i++ {
		lastIndex = proposeWithRetry(t, rc, time.Second, []byte(fmt.Sprintf("v%d", i)))
	}

	require.Eventually(t, func() bool {
		for _, node := range rc.Nodes {
			if node.Status().LastApplied < lastIndex {
				return false
			}
		}
		return true
	}, 2*time.Second, 5*time.Millisecond)

	// Give the snapshotter goroutines a little time to catch up to the
	// latest SnapshotTrigger signals.
	require.Eventually(t, func() bool {
		for _, node := range rc.Nodes {
			if node.Status().LogLen > 2*threshold {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond, "log length should stay bounded near the snapshot threshold, not grow to %d", nEntries)
}

// TestDisconnectedFollowerRecoversViaInstallSnapshot: a follower
// disconnected long enough that the leader compacts past where the
// follower's nextIndex sits can no longer catch up via ordinary
// AppendEntries; it must receive and install a snapshot instead, then
// resume applying the tail of the log normally.
func TestDisconnectedFollowerRecoversViaInstallSnapshot(t *testing.T) {
	const threshold = 5
	rc := testutil.NewRaftCluster(t, 5, 401, withSnapshotThreshold(threshold))
	for _, node := range rc.Nodes {
		serviceSnapshotTrigger(t, node)
	}

	leader := rc.AwaitLeader(t, time.Second)
	laggard := rc.IDs[0]
	for laggard == leader.ID() {
		laggard = rc.IDs[1]
	}
	laggardNode := rc.Nodes[laggard]
	rec := recordApplies(laggardNode)

	rc.Network.Disconnect(laggard)

	const nEntries = 40 // well past the threshold, so the leader compacts.
	var lastIndex uint64
	for i := 0; i < nEntries; i++ {
		lastIndex = proposeWithRetry(t, rc, time.Second, []byte(fmt.Sprintf("v%d", i)))
	}
	require.Eventually(t, func() bool {
		for id, node := range rc.Nodes {
			if id != laggard && node.Status().LastIncludedIndex == 0 {
				return false
			}
		}
		return true
	}, 2*time.Second, 5*time.Millisecond, "reachable nodes should have snapshotted and compacted their logs by now")

	rc.Network.Reconnect(laggard)

	// The laggard must receive a snapshot (it fell behind the leader's
	// compaction point), not just a longer and longer AppendEntries.
	var sawSnapshot raft.ApplyMsg
	require.Eventually(t, func() bool {
		for _, msg := range rec.snapshot() {
			if msg.SnapshotValid {
				sawSnapshot = msg
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond, "laggard should have installed a snapshot")
	require.Greater(t, sawSnapshot.SnapshotIndex, uint64(0))

	// And it must still catch up to the leader's latest entry afterward.
	require.Eventually(t, func() bool {
		return laggardNode.Status().LastApplied >= lastIndex
	}, 2*time.Second, 5*time.Millisecond)
}

// TestRestartFromSnapshotPlusTailMatchesAcrossNodes: after some entries are
// snapshotted away and more are appended on top, restarting every node from
// disk reconstructs the same state everywhere. Snapshotting is a purely
// local, asynchronous decision each node makes on its own schedule, so
// nodes can legitimately have snapshotted at different boundaries — what
// must be identical is the reconstructed command sequence: each node's
// tail must continue with no gap right after its own snapshot boundary,
// contain exactly the commands actually committed at those indices, and
// every node must converge to the same final index.
func TestRestartFromSnapshotPlusTailMatchesAcrossNodes(t *testing.T) {
	const threshold = 5
	rc := testutil.NewRaftCluster(t, 5, 402, diskStorageOpt(t), withSnapshotThreshold(threshold), withGenerousTimeouts())
	for _, node := range rc.Nodes {
		serviceSnapshotTrigger(t, node)
	}

	rc.AwaitLeader(t, time.Second)
	canonical := make(map[uint64][]byte)
	const nBeforeRestart = 20
	var lastIndex uint64
	for i := 0; i < nBeforeRestart; i++ {
		cmd := []byte(fmt.Sprintf("v%d", i))
		idx := proposeWithRetry(t, rc, 2*time.Second, cmd)
		canonical[idx] = cmd
		lastIndex = idx
	}
	for _, node := range rc.Nodes {
		require.Eventually(t, func() bool {
			return node.Status().LastApplied >= lastIndex
		}, 2*time.Second, 5*time.Millisecond)
	}
	require.Eventually(t, func() bool {
		for _, node := range rc.Nodes {
			if node.Status().LastIncludedIndex == 0 {
				return false
			}
		}
		return true
	}, 2*time.Second, 5*time.Millisecond, "every node should have snapshotted before restart")

	for _, id := range rc.IDs {
		rc.Restart(t, id)
	}

	type observed struct {
		snapshotIndex uint64
		tail          []raft.ApplyMsg
	}
	results := make(map[transport.NodeID]*observed, len(rc.Nodes))
	var wg sync.WaitGroup
	for id, node := range rc.Nodes {
		node := node
		obs := &observed{}
		results[id] = obs
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range node.ApplyCh() {
				if msg.SnapshotValid {
					obs.snapshotIndex = msg.SnapshotIndex
					continue
				}
				obs.tail = append(obs.tail, msg)
			}
		}()
	}

	rc.AwaitLeader(t, 2*time.Second)
	postCmd := []byte("post-restart")
	idxPost := proposeWithRetry(t, rc, 2*time.Second, postCmd)
	canonical[idxPost] = postCmd

	for _, node := range rc.Nodes {
		require.Eventually(t, func() bool {
			return node.Status().LastApplied >= idxPost
		}, 2*time.Second, 5*time.Millisecond)
	}
	for _, id := range rc.IDs {
		rc.Nodes[id].Stop()
	}
	wg.Wait()

	for id, obs := range results {
		require.NotZero(t, obs.snapshotIndex, "node %s should have loaded a snapshot on restart", id)
		require.LessOrEqual(t, obs.snapshotIndex, lastIndex, "node %s snapshot boundary should not exceed what existed before restart", id)
		require.NotEmpty(t, obs.tail, "node %s should have applied at least the post-restart entry", id)
		require.Equal(t, obs.snapshotIndex+1, obs.tail[0].CommandIndex, "node %s tail should continue immediately after its snapshot boundary, no gap", id)

		for _, msg := range obs.tail {
			want, ok := canonical[msg.CommandIndex]
			require.True(t, ok, "node %s applied unknown index %d", id, msg.CommandIndex)
			require.Equal(t, want, msg.Command, "node %s command at index %d", id, msg.CommandIndex)
		}
		last := obs.tail[len(obs.tail)-1]
		require.Equal(t, idxPost, last.CommandIndex, "node %s should have caught all the way up to the post-restart entry", id)
	}
}

// drainApplyCh discards everything a node applies, for tests that only
// care about Status(), not the applied sequence.
func drainApplyCh(node *raft.Node) {
	go func() {
		for range node.ApplyCh() {
		}
	}()
}
