package supervisor_test

// These tests exercise the one claim the console rests on: that a node can
// be destroyed and rebuilt in place, and comes back with everything it had
// durably committed. They run the real stack — real listeners, real gRPC,
// real fsynced write-ahead logs — because a crash that doesn't close real
// file handles isn't a crash.

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sujalbistaa/DistKV/internal/supervisor"
	"github.com/sujalbistaa/DistKV/internal/transport"
	"github.com/sujalbistaa/DistKV/internal/transport/grpctransport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

type testNode struct {
	id    transport.NodeID
	addr  string
	sup   *supervisor.Supervisor
	admin pb.AdminClient
	kv    pb.KVClient
}

// startCluster brings up n supervised nodes on loopback and returns them
// with clients already dialed.
func startCluster(t *testing.T, n int) []*testNode {
	t.Helper()

	ids := make([]transport.NodeID, n)
	addrs := make(map[transport.NodeID]string, n)
	listeners := make([]net.Listener, n)
	for i := range ids {
		ids[i] = transport.NodeID(fmt.Sprintf("node-%d", i))
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = lis
		addrs[ids[i]] = lis.Addr().String()
	}

	nodes := make([]*testNode, n)
	for i, id := range ids {
		peers := make([]transport.NodeID, 0, n-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		tr := grpctransport.New(id, addrs)
		sup, err := supervisor.New(supervisor.Config{
			ID:        id,
			Peers:     peers,
			Addrs:     addrs,
			DataDir:   t.TempDir(), // stable across a crash and restart, exactly as a volume would be
			Transport: tr,
		})
		require.NoError(t, err)

		srv := grpc.NewServer()
		pb.RegisterRaftServer(srv, tr)
		pb.RegisterKVServer(srv, sup)
		pb.RegisterAdminServer(srv, sup)
		go func(lis net.Listener) { _ = srv.Serve(lis) }(listeners[i])

		conn, err := grpc.NewClient(addrs[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)

		nodes[i] = &testNode{
			id:    id,
			addr:  addrs[id],
			sup:   sup,
			admin: pb.NewAdminClient(conn),
			kv:    pb.NewKVClient(conn),
		}

		t.Cleanup(func() {
			_ = conn.Close()
			srv.Stop()
			sup.Close()
			_ = tr.Close()
		})
	}
	return nodes
}

func (n *testNode) status(t *testing.T) *pb.NodeStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := n.admin.Status(ctx, &pb.StatusRequest{})
	require.NoError(t, err)
	return st
}

// awaitLeader waits for exactly one node to be reporting itself leader and
// returns it.
func awaitLeader(t *testing.T, nodes []*testNode) *testNode {
	t.Helper()
	var leader *testNode
	require.Eventually(t, func() bool {
		leader = nil
		for _, n := range nodes {
			st := n.status(t)
			if st.Lifecycle == pb.Lifecycle_LIFECYCLE_RUNNING && st.Role == "leader" {
				leader = n
			}
		}
		return leader != nil
	}, 15*time.Second, 25*time.Millisecond, "no leader elected")
	return leader
}

// put writes through whichever node is leader, following redirects the way
// any client would.
func put(t *testing.T, nodes []*testNode, clientID string, seq uint64, key, value string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			reply, err := n.kv.Put(ctx, &pb.Command{ClientId: clientID, SeqNo: seq, Key: key, Value: value})
			cancel()
			if err == nil && reply.Success {
				return true
			}
		}
		return false
	}, 15*time.Second, 50*time.Millisecond, "no node accepted the write")
}

// TestCrashedNodeRecoversFromItsLog is the whole demo in one test: kill the
// leader outright, watch the rest elect a replacement and keep serving,
// then bring the dead node back and confirm it recovers the write it was
// holding when it died — from its own write-ahead log, since that is the
// only place it could have come from.
func TestCrashedNodeRecoversFromItsLog(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := awaitLeader(t, nodes)

	put(t, nodes, "recovery", 1, "city", "kathmandu")

	crashed := leader
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := crashed.admin.Crash(ctx, &pb.LifecycleRequest{})
	cancel()
	require.NoError(t, err)

	// A crashed node is reachable and honest about being down, and refuses
	// client work rather than answering from stale state.
	require.Equal(t, pb.Lifecycle_LIFECYCLE_CRASHED, crashed.status(t).Lifecycle)
	reqCtx, reqCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = crashed.kv.Get(reqCtx, &pb.Command{ClientId: "probe", SeqNo: 1, Key: "city"})
	reqCancel()
	require.Error(t, err, "a crashed node must not serve reads")

	// The survivors carry on: they elect someone and accept a new write.
	survivors := make([]*testNode, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.id != crashed.id {
			survivors = append(survivors, n)
		}
	}
	awaitLeader(t, survivors)
	put(t, survivors, "recovery", 2, "region", "bagmati")

	// Now bring it back. Everything it knows it must have read off disk.
	restartCtx, restartCancel := context.WithTimeout(context.Background(), 5*time.Second)
	reply, err := crashed.admin.Restart(restartCtx, &pb.LifecycleRequest{})
	restartCancel()
	require.NoError(t, err)
	require.Empty(t, reply.Error)
	require.Equal(t, pb.Lifecycle_LIFECYCLE_RUNNING, reply.Lifecycle)

	require.Eventually(t, func() bool {
		st := crashed.status(t)
		// Two keys: the one it wrote before dying, recovered from its log,
		// and the one written while it was gone, replicated to it on
		// rejoining.
		return st.Lifecycle == pb.Lifecycle_LIFECYCLE_RUNNING && st.KeyCount == 2
	}, 15*time.Second, 50*time.Millisecond, "the restarted node should recover its state and catch up")
}

// TestIsolatedNodeCannotWinAnElection covers the other half: a node cut off
// from the cluster campaigns forever and never wins, while the majority
// that can still hear each other carries on undisturbed.
func TestIsolatedNodeCannotWinAnElection(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := awaitLeader(t, nodes)
	put(t, nodes, "partition", 1, "city", "kathmandu")

	// Cut off a follower rather than the leader. A leader that loses
	// contact with its followers does not campaign — it goes on believing
	// it leads, and simply stops being able to commit anything, which is a
	// different behaviour worth not conflating with this one.
	var isolated *testNode
	var peers []string
	for _, n := range nodes {
		if n.id == leader.id {
			continue
		}
		if isolated == nil {
			isolated = n
			continue
		}
		peers = append(peers, string(n.id))
	}
	peers = append(peers, string(leader.id))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := isolated.admin.SetPartition(ctx, &pb.SetPartitionRequest{Peers: peers})
	cancel()
	require.NoError(t, err)

	// It stands for election over and over, driving its term up, and wins
	// nothing: two of three nodes are unreachable, so it can never
	// assemble a majority.
	startTerm := isolated.status(t).Term
	require.Eventually(t, func() bool {
		st := isolated.status(t)
		return st.Term > startTerm+2 && st.Role != "leader"
	}, 10*time.Second, 25*time.Millisecond, "an isolated node should campaign without ever winning")

	// The other two still have a leader between them and still accept
	// writes, at a term far below the one the isolated node has reached.
	majority := make([]*testNode, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.id != isolated.id {
			majority = append(majority, n)
		}
	}
	majorityLeader := awaitLeader(t, majority)
	put(t, majority, "partition", 2, "region", "bagmati")
	require.Less(t, majorityLeader.status(t).Term, isolated.status(t).Term,
		"the cut-off node's term should have outrun the cluster's")

	// Heal, and it comes back into the fold with the write it missed.
	healCtx, healCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = isolated.admin.SetPartition(healCtx, &pb.SetPartitionRequest{})
	healCancel()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return isolated.status(t).KeyCount == 2
	}, 15*time.Second, 50*time.Millisecond, "a healed node should catch up on what it missed")
}
