package grpctransport_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sujalbistaa/DistKV/internal/kv"
	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/storage"
	"github.com/sujalbistaa/DistKV/internal/transport"
	"github.com/sujalbistaa/DistKV/internal/transport/grpctransport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

type grpcNode struct {
	id       transport.NodeID
	addr     string
	node     *raft.Node
	kvServer *kv.Server
	grpcSrv  *grpc.Server
}

// startCluster brings up n nodes, each a real process-like unit: its own
// listener, its own gRPC server carrying both the Raft and KV services,
// its own on-disk write-ahead log, all talking over real TCP.
func startCluster(t *testing.T, n int) []*grpcNode {
	t.Helper()

	ids := make([]transport.NodeID, n)
	listeners := make([]net.Listener, n)
	addrs := make(map[transport.NodeID]string, n)
	for i := 0; i < n; i++ {
		ids[i] = transport.NodeID(fmt.Sprintf("node-%d", i))
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = lis
		addrs[ids[i]] = lis.Addr().String()
	}

	nodes := make([]*grpcNode, n)
	for i, id := range ids {
		peers := make([]transport.NodeID, 0, n-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		tr := grpctransport.New(id, addrs)
		ds, err := storage.NewDiskStorage(t.TempDir())
		require.NoError(t, err)

		node, err := raft.NewNode(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          tr,
			Storage:            ds,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			TickInterval:       10 * time.Millisecond,
			RPCTimeout:         100 * time.Millisecond,
			Seed:               int64(i) + 1,
		})
		require.NoError(t, err)

		kvSrv := kv.NewServer(node, kv.NewStore())

		grpcSrv := grpc.NewServer()
		pb.RegisterRaftServer(grpcSrv, tr)
		pb.RegisterKVServer(grpcSrv, kvSrv)
		go func(lis net.Listener) { _ = grpcSrv.Serve(lis) }(listeners[i])

		nodes[i] = &grpcNode{id: id, addr: addrs[id], node: node, kvServer: kvSrv, grpcSrv: grpcSrv}

		t.Cleanup(func() {
			grpcSrv.Stop()
			kvSrv.Stop()
			node.Stop()
			_ = tr.Close()
			_ = ds.Close()
		})
	}
	return nodes
}

func awaitLeader(t *testing.T, nodes []*grpcNode, timeout time.Duration) *grpcNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if _, role := n.node.State(); role == raft.Leader {
				return n
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no leader elected over gRPC within %s", timeout)
	return nil
}

// TestClusterOverRealGRPC runs the whole stack the way it runs in
// production — real TCP, real protobuf, real fsynced write-ahead logs —
// and checks that it elects a leader, replicates, and serves the KV API
// over the wire.
func TestClusterOverRealGRPC(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := awaitLeader(t, nodes, 10*time.Second)

	conn, err := grpc.NewClient(leader.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reply, err := client.Put(ctx, &pb.Command{ClientId: "grpc-e2e", SeqNo: 1, Key: "city", Value: "kathmandu"})
	require.NoError(t, err)
	require.True(t, reply.Success, "put failed: %s", reply.Error)

	reply, err = client.Append(ctx, &pb.Command{ClientId: "grpc-e2e", SeqNo: 2, Key: "city", Value: "-nepal"})
	require.NoError(t, err)
	require.True(t, reply.Success)

	reply, err = client.Get(ctx, &pb.Command{ClientId: "grpc-e2e", SeqNo: 3, Key: "city"})
	require.NoError(t, err)
	require.True(t, reply.Success)
	require.True(t, reply.Found)
	require.Equal(t, "kathmandu-nepal", reply.Value)

	// The write really replicated: every node's state machine, not just
	// the leader's, can answer for it once it becomes leader. Check the
	// durable path instead by reading through each node and accepting a
	// redirect from the followers.
	for _, n := range nodes {
		nodeConn, err := grpc.NewClient(n.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		r, err := pb.NewKVClient(nodeConn).Get(ctx, &pb.Command{ClientId: "grpc-e2e-probe", SeqNo: 1, Key: "city"})
		require.NoError(t, err)
		if n.id == leader.id {
			require.True(t, r.Success)
			require.Equal(t, "kathmandu-nepal", r.Value)
		} else {
			require.False(t, r.Success, "a follower must redirect rather than serve")
			require.Equal(t, string(leader.id), r.LeaderHint)
		}
		_ = nodeConn.Close()
	}
}

// TestLeaderFailoverOverRealGRPC kills the leader's server and confirms
// the remaining nodes elect a new one and keep serving, over real
// connections rather than a simulated network.
func TestLeaderFailoverOverRealGRPC(t *testing.T) {
	nodes := startCluster(t, 3)
	leader := awaitLeader(t, nodes, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(leader.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	reply, err := pb.NewKVClient(conn).Put(ctx, &pb.Command{ClientId: "failover", SeqNo: 1, Key: "k", Value: "before"})
	require.NoError(t, err)
	require.True(t, reply.Success)
	_ = conn.Close()

	// Take the leader's server down hard.
	leader.grpcSrv.Stop()
	leader.node.Stop()

	var survivor *grpcNode
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if n.id == leader.id {
				continue
			}
			if _, role := n.node.State(); role == raft.Leader {
				survivor = n
				return true
			}
		}
		return false
	}, 15*time.Second, 20*time.Millisecond, "surviving nodes should elect a new leader")

	newConn, err := grpc.NewClient(survivor.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = newConn.Close() })
	newClient := pb.NewKVClient(newConn)

	require.Eventually(t, func() bool {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 2*time.Second)
		defer attemptCancel()
		r, err := newClient.Get(attemptCtx, &pb.Command{ClientId: "failover", SeqNo: 2, Key: "k"})
		return err == nil && r.Success && r.Value == "before"
	}, 15*time.Second, 100*time.Millisecond, "the new leader should still hold the pre-failover write")
}
