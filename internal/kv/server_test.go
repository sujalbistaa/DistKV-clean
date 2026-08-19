package kv_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/internal/kv"
	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/testutil"
	"github.com/sujalbistaa/DistKV/internal/transport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// kvCluster is a RaftCluster with a kv.Server (and its own Store) wired
// onto every node.
type kvCluster struct {
	*testutil.RaftCluster
	Servers map[transport.NodeID]*kv.Server
}

func newKVCluster(t *testing.T, n int, seed int64, opts ...testutil.RaftClusterOpt) *kvCluster {
	t.Helper()
	rc := testutil.NewRaftCluster(t, n, seed, opts...)
	kc := &kvCluster{RaftCluster: rc, Servers: make(map[transport.NodeID]*kv.Server, n)}
	for id, node := range rc.Nodes {
		kc.Servers[id] = kv.NewServer(node, kv.NewStore())
	}
	t.Cleanup(func() {
		for _, srv := range kc.Servers {
			srv.Stop()
		}
	})
	return kc
}

// awaitLeaderServer waits for a Raft leader and returns its kv.Server.
func (kc *kvCluster) awaitLeaderServer(t *testing.T, timeout time.Duration) (transport.NodeID, *kv.Server) {
	t.Helper()
	leader := kc.AwaitLeader(t, timeout)
	return leader.ID(), kc.Servers[leader.ID()]
}

// currentLeaderServer returns the kv.Server for whichever node currently
// reports itself Raft leader, or ("", nil) if none does right now.
func (kc *kvCluster) currentLeaderServer() (transport.NodeID, *kv.Server) {
	for id, node := range kc.Nodes {
		if _, role := node.State(); role == raft.Leader {
			return id, kc.Servers[id]
		}
	}
	return "", nil
}

// executeWithRetry calls call against whichever node currently claims
// leadership, bounding each individual attempt to a short per-attempt
// timeout and retrying against a possibly-different leader until one
// succeeds or the overall timeout elapses. This is what a real client
// does, and it's necessary in tests that deliberately induce leader
// churn: a single attempt against a leader that turns out to be
// transient (about to be deposed again) could otherwise hang forever
// against an unbounded context waiting for an entry that will never
// commit under that leader's term.
func executeWithRetry(t *testing.T, kc *kvCluster, timeout time.Duration, call func(ctx context.Context, srv *kv.Server) (*pb.CommandReply, error)) *pb.CommandReply {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, srv := kc.currentLeaderServer(); srv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			reply, err := call(ctx, srv)
			cancel()
			if err == nil && reply.Success {
				return reply
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("execute did not succeed within %s", timeout)
	return nil
}

// TestRetryAcrossFailoverAppliesAppendExactlyOnce: a client's Append is
// durably proposed but the client gives up waiting before seeing the
// reply (simulating a lost response); the leader is then crashed
// (failover) and the client retries the identical request, by client id
// and sequence number, against the new leader. The append must show up in
// the final value exactly once either way.
func TestRetryAcrossFailoverAppliesAppendExactlyOnce(t *testing.T) {
	kc := newKVCluster(t, 5, 500)
	leaderID, leaderSrv := kc.awaitLeaderServer(t, time.Second)

	cmd := &pb.Command{ClientId: "client-1", SeqNo: 1, Key: "k", Value: "a"}

	// The client gives up almost immediately; Propose has already
	// happened synchronously inside execute by the time the context
	// expires, so the entry is durably in the leader's log and will
	// commit on its own regardless of whether anyone is still listening.
	shortCtx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	_, err := leaderSrv.Put(shortCtx, cmd) // Put/Append/Delete all execute the same way; op is irrelevant here except that it mutates.
	cancel()
	require.Error(t, err)

	require.Eventually(t, func() bool {
		reply, err := leaderSrv.Get(context.Background(), &pb.Command{ClientId: "reader", SeqNo: 1, Key: "k"})
		return err == nil && reply.Success && reply.Value == "a"
	}, time.Second, 5*time.Millisecond, "the append should have committed in the background despite the client giving up")

	// Now force an actual failover and retry the identical request.
	kc.Network.Crash(leaderID)
	require.Eventually(t, func() bool {
		id, _ := kc.currentLeaderServer()
		return id != "" && id != leaderID
	}, time.Second, 5*time.Millisecond, "cluster should elect a new leader after the old one crashes")

	reply := executeWithRetry(t, kc, 3*time.Second, func(ctx context.Context, srv *kv.Server) (*pb.CommandReply, error) {
		return srv.Put(ctx, &pb.Command{ClientId: "client-1", SeqNo: 1, Key: "k", Value: "a"})
	})
	require.True(t, reply.Success)

	final := executeWithRetry(t, kc, 3*time.Second, func(ctx context.Context, srv *kv.Server) (*pb.CommandReply, error) {
		return srv.Get(ctx, &pb.Command{ClientId: "reader", SeqNo: 2, Key: "k"})
	})
	require.Equal(t, "a", final.Value, "the append must not have been applied a second time")
}

// TestReadsNeverStaleAcrossPartition: a value committed before a partition
// is updated on the majority side while the old leader is isolated in the
// minority. A Get sent to the isolated old leader must never return the
// stale pre-partition value — since Get is proposed through the log like
// any write, it can only ever return a truly committed value, and here it
// can't commit at all while isolated, so it must fail rather than answer
// wrong. Once healed, a Get against the real leader returns the new
// value.
func TestReadsNeverStaleAcrossPartition(t *testing.T) {
	kc := newKVCluster(t, 5, 501)
	oldLeaderID, oldLeaderSrv := kc.awaitLeaderServer(t, time.Second)

	reply, err := oldLeaderSrv.Put(context.Background(), &pb.Command{ClientId: "writer", SeqNo: 1, Key: "k", Value: "v1"})
	require.NoError(t, err)
	require.True(t, reply.Success)

	var rest []transport.NodeID
	for _, id := range kc.IDs {
		if id != oldLeaderID {
			rest = append(rest, id)
		}
	}
	kc.Partition([]transport.NodeID{oldLeaderID}, rest)

	var newLeaderID transport.NodeID
	var newLeaderSrv *kv.Server
	require.Eventually(t, func() bool {
		for _, id := range rest {
			if _, role := kc.Nodes[id].State(); role == raft.Leader {
				newLeaderID, newLeaderSrv = id, kc.Servers[id]
				return true
			}
		}
		return false
	}, time.Second, 5*time.Millisecond, "majority side should elect a new leader")

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		r, err := newLeaderSrv.Put(ctx, &pb.Command{ClientId: "writer", SeqNo: 2, Key: "k", Value: "v2"})
		return err == nil && r.Success
	}, 3*time.Second, 10*time.Millisecond, "majority side should accept the new write")

	// The isolated old leader can never commit a Get (it can't reach a
	// majority), so it must fail rather than silently answer with its
	// stale local "v1".
	staleCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, err = oldLeaderSrv.Get(staleCtx, &pb.Command{ClientId: "reader", SeqNo: 1, Key: "k"})
	cancel()
	require.Error(t, err, "a read on the isolated old leader must not succeed with stale data")

	kc.Heal()

	require.Eventually(t, func() bool {
		r, err := newLeaderSrv.Get(context.Background(), &pb.Command{ClientId: "reader", SeqNo: 2, Key: "k"})
		return err == nil && r.Success && r.Value == "v2"
	}, time.Second, 5*time.Millisecond)

	_ = newLeaderID
}

// TestGetReflectsPriorCompletedWrite is a smaller, direct check that a Get
// issued after a Put has been acknowledged as successful always observes
// it, on any node that has caught up.
func TestGetReflectsPriorCompletedWrite(t *testing.T) {
	kc := newKVCluster(t, 3, 502)
	_, leaderSrv := kc.awaitLeaderServer(t, time.Second)

	reply, err := leaderSrv.Put(context.Background(), &pb.Command{ClientId: "writer", SeqNo: 1, Key: "x", Value: "1"})
	require.NoError(t, err)
	require.True(t, reply.Success)

	got, err := leaderSrv.Get(context.Background(), &pb.Command{ClientId: "reader", SeqNo: 1, Key: "x"})
	require.NoError(t, err)
	require.True(t, got.Success)
	require.True(t, got.Found)
	require.Equal(t, "1", got.Value)
}

// TestNonLeaderRedirects: a request sent to a follower is rejected with a
// hint pointing at the current leader instead of being silently dropped
// or, worse, served locally.
func TestNonLeaderRedirects(t *testing.T) {
	kc := newKVCluster(t, 3, 503)
	leaderID, _ := kc.awaitLeaderServer(t, time.Second)

	var followerID transport.NodeID
	for _, id := range kc.IDs {
		if id != leaderID {
			followerID = id
			break
		}
	}
	// Give the follower a moment to have processed at least one heartbeat
	// from the leader; until it has, it doesn't yet know who to redirect
	// to.
	require.Eventually(t, func() bool {
		return kc.Nodes[followerID].Leader() == leaderID
	}, time.Second, 2*time.Millisecond)

	reply, err := kc.Servers[followerID].Get(context.Background(), &pb.Command{ClientId: "reader", SeqNo: 1, Key: "x"})
	require.NoError(t, err)
	require.False(t, reply.Success)
	require.Equal(t, string(leaderID), reply.LeaderHint)
}

// TestRedirectCarriesDialableAddress: when the server is configured with
// peer addresses, a refusal names an address the client can actually
// connect to rather than a bare node id.
//
// Regression test. Without this, a client given a single endpoint can
// never reach a leader that happens to be a different node: it is told
// "the leader is node3", which is not something it can dial. This was
// found by running the documented Docker quickstart, where it made the
// README's own example fail.
func TestRedirectCarriesDialableAddress(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 3, 504)

	addrs := make(map[transport.NodeID]string, len(rc.IDs))
	for i, id := range rc.IDs {
		addrs[id] = fmt.Sprintf("10.0.0.%d:7070", i+1)
	}

	servers := make(map[transport.NodeID]*kv.Server, len(rc.Nodes))
	for id, node := range rc.Nodes {
		srv := kv.NewServer(node, kv.NewStore(), kv.WithLeaderAddresses(addrs))
		servers[id] = srv
		t.Cleanup(srv.Stop)
	}

	leader := rc.AwaitLeader(t, time.Second)
	var followerID transport.NodeID
	for _, id := range rc.IDs {
		if id != leader.ID() {
			followerID = id
			break
		}
	}
	require.Eventually(t, func() bool {
		return rc.Nodes[followerID].Leader() == leader.ID()
	}, time.Second, 2*time.Millisecond)

	reply, err := servers[followerID].Get(context.Background(), &pb.Command{ClientId: "reader", SeqNo: 1, Key: "x"})
	require.NoError(t, err)
	require.False(t, reply.Success)
	require.Equal(t, addrs[leader.ID()], reply.LeaderHint,
		"the hint must be a dialable address, not a node id the client cannot resolve")
}
