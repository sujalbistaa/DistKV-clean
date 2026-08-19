package raft_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/storage"
	"github.com/sujalbistaa/DistKV/internal/testutil"
	"github.com/sujalbistaa/DistKV/internal/transport"
)

// diskStorageOpt gives each node its own on-disk write-ahead log under a
// fresh temp directory, instead of the default in-memory Storage, so a
// RaftCluster built with it can survive a real Restart.
func diskStorageOpt(t *testing.T) testutil.RaftClusterOpt {
	return func(i int, cfg *raft.Config) {
		ds, err := storage.NewDiskStorage(t.TempDir())
		if err != nil {
			t.Fatalf("storage.NewDiskStorage: %v", err)
		}
		cfg.Storage = ds
	}
}

// TestCrashAndRestartPreservesCommittedEntries: every node in the cluster
// is stopped and reconstructed from its own on-disk state (a real crash
// and restart, not just a network-level Crash), and the entries committed
// before the crash are still there afterward.
func TestCrashAndRestartPreservesCommittedEntries(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 300, diskStorageOpt(t))

	preRecorders := make(map[transport.NodeID]*applyRecorder, len(rc.Nodes))
	for id, node := range rc.Nodes {
		preRecorders[id] = recordApplies(node)
	}

	leader := rc.AwaitLeader(t, time.Second)
	const nPre = 3
	var lastPreIndex uint64
	for i := 0; i < nPre; i++ {
		idx, _, ok := leader.Propose([]byte(fmt.Sprintf("pre-%d", i)))
		require.True(t, ok)
		lastPreIndex = idx
	}
	for _, rec := range preRecorders {
		rec.waitForIndex(t, lastPreIndex, time.Second)
	}

	for _, id := range rc.IDs {
		rc.Restart(t, id)
	}

	postRecorders := make(map[transport.NodeID]*applyRecorder, len(rc.Nodes))
	for id, node := range rc.Nodes {
		postRecorders[id] = recordApplies(node)
	}

	// A restarted cluster with no new writes cannot safely know its old
	// commitIndex was real (that's volatile state, lost on crash by
	// design — Raft paper, Figure 2). It only re-establishes it once a
	// new leader commits something in its own term; this proposal is that
	// nudge, standard practice for exercising crash recovery.
	newLeader := rc.AwaitLeader(t, 2*time.Second)
	idxPost, _, ok := newLeader.Propose([]byte("post-restart"))
	require.True(t, ok)
	require.Greater(t, idxPost, lastPreIndex)

	for id, rec := range postRecorders {
		rec.waitForIndex(t, idxPost, 2*time.Second)
		applied := rec.snapshot()
		require.GreaterOrEqual(t, len(applied), int(lastPreIndex), "node %s lost committed entries across restart", id)
		for i := 0; i < nPre; i++ {
			require.Equal(t, []byte(fmt.Sprintf("pre-%d", i)), applied[i].Command, "node %s pre-crash entry %d", id, i)
		}
	}
}

// TestNodeRecoversFromTornWALOnRestart simulates a crash mid-fsync: the
// on-disk log has two complete, valid entries followed by a record header
// promising a payload that was never fully written. A Node built on top of
// this storage must recover exactly the two valid entries and continue
// from there — proving the torn-tail repair from the storage package is
// correctly wired into raft.NewNode, not just correct in isolation.
func TestNodeRecoversFromTornWALOnRestart(t *testing.T) {
	dir := t.TempDir()
	ds, err := storage.NewDiskStorage(dir)
	require.NoError(t, err)
	require.NoError(t, ds.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("good-1")},
		{Term: 1, Index: 2, Command: []byte("good-2")},
	}))
	require.NoError(t, ds.SaveHardState(1, ""))
	require.NoError(t, ds.Close())

	walPath := filepath.Join(dir, "log.wal")
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	// A record header claiming a payload well beyond what actually
	// follows: exactly what's left behind by a crash mid-write.
	_, err = f.Write([]byte{0, 0, 2, 0, 1, 2, 3, 4, 9, 9})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	ds2, err := storage.NewDiskStorage(dir)
	require.NoError(t, err)

	net := transport.NewNetwork(1)
	tr := net.NewTransport("solo")
	node, err := raft.NewNode(raft.Config{
		ID:                 "solo",
		Transport:          tr,
		Storage:            ds2,
		ElectionTimeoutMin: 10 * time.Millisecond,
		ElectionTimeoutMax: 20 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
		TickInterval:       2 * time.Millisecond,
	})
	require.NoError(t, err)
	defer node.Stop()

	// Single-node cluster: elects itself with no peers to wait on.
	require.Eventually(t, func() bool {
		_, role := node.State()
		return role == raft.Leader
	}, time.Second, 2*time.Millisecond)

	index, _, ok := node.Propose([]byte("new"))
	require.True(t, ok)
	require.Equal(t, uint64(3), index, "recovered log should hold exactly the 2 valid entries, so the next index is 3")
}

// TestNodeDoesNotDoubleVoteAfterRestart: a node that grants a vote in term
// T, then crashes and restarts, must still refuse to grant a second,
// different vote in that same term T.
func TestNodeDoesNotDoubleVoteAfterRestart(t *testing.T) {
	dir := t.TempDir()
	net := transport.NewNetwork(1)
	ctx := context.Background()

	newVoter := func(ds raft.Storage) *raft.Node {
		trans := net.NewTransport("voter")
		node, err := raft.NewNode(raft.Config{
			ID:        "voter",
			Peers:     []transport.NodeID{"asker"},
			Transport: trans,
			Storage:   ds,
			// Long enough that the voter never times out and starts its
			// own election during this test; only its RPC handling and
			// persistence matter here.
			ElectionTimeoutMin: time.Hour,
			ElectionTimeoutMax: 2 * time.Hour,
			HeartbeatInterval:  time.Hour,
			TickInterval:       5 * time.Millisecond,
		})
		require.NoError(t, err)
		return node
	}

	ds, err := storage.NewDiskStorage(dir)
	require.NoError(t, err)
	voter := newVoter(ds)

	asker := net.NewTransport("asker")
	resp, err := asker.Send(ctx, "voter", &raft.RequestVoteArgs{
		Term: 5, CandidateID: "candidate-A", LastLogIndex: 0, LastLogTerm: 0,
	})
	require.NoError(t, err)
	require.True(t, resp.(*raft.RequestVoteReply).VoteGranted)

	voter.Stop()
	require.NoError(t, ds.Close())

	ds2, err := storage.NewDiskStorage(dir)
	require.NoError(t, err)
	voter2 := newVoter(ds2)
	defer voter2.Stop()

	resp2, err := asker.Send(ctx, "voter", &raft.RequestVoteArgs{
		Term: 5, CandidateID: "candidate-B", LastLogIndex: 0, LastLogTerm: 0,
	})
	require.NoError(t, err)
	require.False(t, resp2.(*raft.RequestVoteReply).VoteGranted,
		"must not grant a second vote in the same term after restart")
}
