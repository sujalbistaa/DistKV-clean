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

// applyRecorder collects every ApplyMsg a node delivers, from the moment it
// is created, so tests can assert on order, gaps, and duplicates without
// racing the node's own goroutines.
type applyRecorder struct {
	mu      sync.Mutex
	applied []raft.ApplyMsg
	done    chan struct{}
}

func recordApplies(node *raft.Node) *applyRecorder {
	r := &applyRecorder{done: make(chan struct{})}
	go func() {
		defer close(r.done)
		for msg := range node.ApplyCh() {
			r.mu.Lock()
			r.applied = append(r.applied, msg)
			r.mu.Unlock()
		}
	}()
	return r
}

func (r *applyRecorder) snapshot() []raft.ApplyMsg {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]raft.ApplyMsg(nil), r.applied...)
}

// waitForIndex blocks until index has been applied (or a later index has,
// which is impossible without index having been applied first given
// in-order delivery), returning its ApplyMsg.
func (r *applyRecorder) waitForIndex(t testing.TB, index uint64, timeout time.Duration) raft.ApplyMsg {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, msg := range r.snapshot() {
			if msg.CommandIndex == index {
				return msg
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("index %d was never applied within %s", index, timeout)
	return raft.ApplyMsg{}
}

func proposeOnLeader(t testing.TB, rc *testutil.RaftCluster, timeout time.Duration, cmd []byte) (*raft.Node, uint64, uint64) {
	t.Helper()
	leader := rc.AwaitLeader(t, timeout)
	index, term, ok := leader.Propose(cmd)
	require.True(t, ok, "Propose on a node AwaitLeader just confirmed is leader should succeed")
	return leader, index, term
}

// TestAgreementAllNodesUp: a proposed value is agreed on and applied, in
// order, by every node in a healthy 5-node cluster.
func TestAgreementAllNodesUp(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 200)
	recorders := make(map[transport.NodeID]*applyRecorder, len(rc.Nodes))
	for id, node := range rc.Nodes {
		recorders[id] = recordApplies(node)
	}

	_, index, term := proposeOnLeader(t, rc, time.Second, []byte("hello"))
	require.Equal(t, uint64(1), index)

	for id, rec := range recorders {
		msg := rec.waitForIndex(t, index, time.Second)
		require.Equal(t, []byte("hello"), msg.Command, "node %s", id)
		require.Equal(t, term, msg.CommandTerm, "node %s", id)
	}
}

// TestAgreementWithDisconnectedFollowerCatchesUp: entries commit with one
// follower disconnected (4 of 5 is still a majority), and once it
// reconnects it catches up to the exact same applied sequence.
func TestAgreementWithDisconnectedFollowerCatchesUp(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 201)
	recorders := make(map[transport.NodeID]*applyRecorder, len(rc.Nodes))
	for id, node := range rc.Nodes {
		recorders[id] = recordApplies(node)
	}

	leader := rc.AwaitLeader(t, time.Second)
	laggard := rc.IDs[0]
	for laggard == leader.ID() {
		laggard = rc.IDs[1]
	}
	rc.Network.Disconnect(laggard)

	var lastIndex uint64
	for i := 0; i < 5; i++ {
		index, _, ok := leader.Propose([]byte(fmt.Sprintf("v%d", i)))
		require.True(t, ok)
		lastIndex = index
	}

	for id, rec := range recorders {
		if id == laggard {
			continue
		}
		rec.waitForIndex(t, lastIndex, time.Second)
	}

	// The disconnected follower must not have applied anything yet.
	require.Empty(t, recorders[laggard].snapshot())

	rc.Network.Reconnect(laggard)
	msg := recorders[laggard].waitForIndex(t, lastIndex, time.Second)
	require.Equal(t, []byte("v4"), msg.Command)

	// Every node's applied sequence for the shared prefix must be
	// identical: same commands at the same indices, same order.
	want := recorders[leader.ID()].snapshot()
	for id, rec := range recorders {
		require.Equal(t, want, rec.snapshot(), "node %s diverged from the leader's applied sequence", id)
	}
}

// TestNoAgreementWithMinority: a leader isolated in a minority partition
// accepts a proposal locally but it never commits, because it can never
// reach a majority.
func TestNoAgreementWithMinority(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 202)
	leader := rc.AwaitLeader(t, time.Second)

	var rest []transport.NodeID
	for _, id := range rc.IDs {
		if id != leader.ID() {
			rest = append(rest, id)
		}
	}
	rc.Partition([]transport.NodeID{leader.ID()}, rest)

	index, _, ok := leader.Propose([]byte("stuck"))
	require.True(t, ok)
	require.Equal(t, uint64(1), index)

	select {
	case msg := <-leader.ApplyCh():
		t.Fatalf("entry committed despite being confined to a minority: %+v", msg)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestConcurrentProposalsAppearExactlyOnce: many goroutines proposing
// concurrently against a stable leader all see their command applied
// exactly once, at a distinct index, with no gaps.
func TestConcurrentProposalsAppearExactlyOnce(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 203)
	recorders := make(map[transport.NodeID]*applyRecorder, len(rc.Nodes))
	for id, node := range rc.Nodes {
		recorders[id] = recordApplies(node)
	}
	leader := rc.AwaitLeader(t, time.Second)

	const n = 50
	var wg sync.WaitGroup
	indices := make([]uint64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := []byte(fmt.Sprintf("cmd-%03d", i))
			index, _, ok := leader.Propose(cmd)
			require.True(t, ok)
			indices[i] = index
		}(i)
	}
	wg.Wait()

	// Proposals were concurrent, so indices form a set, not necessarily
	// assigned in submission order; they must still be exactly 1..n with
	// no collisions.
	seen := make(map[uint64]bool, n)
	for _, idx := range indices {
		require.False(t, seen[idx], "index %d assigned twice", idx)
		seen[idx] = true
	}
	for i := uint64(1); i <= n; i++ {
		require.True(t, seen[i], "index %d was never assigned", i)
	}

	rec := recorders[leader.ID()]
	rec.waitForIndex(t, n, 2*time.Second)

	applied := rec.snapshot()
	require.Len(t, applied, n)
	byCommand := make(map[string]bool, n)
	for i, msg := range applied {
		require.EqualValues(t, i+1, msg.CommandIndex, "gap or reorder in applied sequence")
		require.False(t, byCommand[string(msg.Command)], "command %q applied twice", msg.Command)
		byCommand[string(msg.Command)] = true
	}
	require.Len(t, byCommand, n)
}

// TestConflictingEntriesTruncatedOnRejoin reproduces the Raft paper's
// Figure 7 scenario: an isolated leader appends an entry that can never
// commit, a new leader elected by the majority commits a different entry
// at the same index, and once the partition heals the old leader (and its
// former partner) truncate their conflicting entry and adopt the
// committed one.
func TestConflictingEntriesTruncatedOnRejoin(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 204)
	recorders := make(map[transport.NodeID]*applyRecorder, len(rc.Nodes))
	for id, node := range rc.Nodes {
		recorders[id] = recordApplies(node)
	}

	leaderA := rc.AwaitLeader(t, time.Second)
	termA, _ := leaderA.State()

	var partner transport.NodeID
	for id := range rc.Nodes {
		if id != leaderA.ID() {
			partner = id
			break
		}
	}
	var majority []transport.NodeID
	for _, id := range rc.IDs {
		if id != leaderA.ID() && id != partner {
			majority = append(majority, id)
		}
	}
	minority := []transport.NodeID{leaderA.ID(), partner}
	rc.Partition(minority, majority)

	idxX, _, ok := leaderA.Propose([]byte("X"))
	require.True(t, ok)
	require.Equal(t, uint64(1), idxX)

	// X must never commit: it can reach at most 2 of 5 nodes.
	time.Sleep(200 * time.Millisecond)
	require.Empty(t, recorders[leaderA.ID()].snapshot(), "uncommitted entry X should not have been applied")

	var leaderC *raft.Node
	deadline := time.Now().Add(time.Second)
	for leaderC == nil && time.Now().Before(deadline) {
		for _, id := range majority {
			if term, role := rc.Nodes[id].State(); role == raft.Leader && term > termA {
				leaderC = rc.Nodes[id]
				break
			}
		}
		if leaderC == nil {
			time.Sleep(2 * time.Millisecond)
		}
	}
	require.NotNil(t, leaderC, "majority side failed to elect a new leader")

	idxY, _, ok := leaderC.Propose([]byte("Y"))
	require.True(t, ok)
	require.Equal(t, idxX, idxY, "Y must occupy the same index X did for this to be a real conflict")

	for _, id := range majority {
		msg := recorders[id].waitForIndex(t, idxY, time.Second)
		require.Equal(t, []byte("Y"), msg.Command, "node %s", id)
	}

	rc.Heal()

	for _, id := range minority {
		msg := recorders[id].waitForIndex(t, idxY, time.Second)
		require.Equal(t, []byte("Y"), msg.Command, "node %s should have truncated X and adopted Y", id)
	}
}
