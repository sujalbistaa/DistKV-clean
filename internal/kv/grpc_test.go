package kv_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// TestGRPCServiceEndToEnd runs the real generated gRPC service over a real
// TCP connection against a real cluster, so the wire path — not just the
// in-process Go API the other tests exercise — is known to work.
func TestGRPCServiceEndToEnd(t *testing.T) {
	kc := newKVCluster(t, 3, 600)
	leaderID, leaderSrv := kc.awaitLeaderServer(t, time.Second)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcSrv := grpc.NewServer()
	pb.RegisterKVServer(grpcSrv, leaderSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := client.Put(ctx, &pb.Command{ClientId: "grpc-client", SeqNo: 1, Key: "greeting", Value: "hello"})
	require.NoError(t, err)
	require.True(t, reply.Success)

	reply, err = client.Append(ctx, &pb.Command{ClientId: "grpc-client", SeqNo: 2, Key: "greeting", Value: " world"})
	require.NoError(t, err)
	require.True(t, reply.Success)

	reply, err = client.Get(ctx, &pb.Command{ClientId: "grpc-client", SeqNo: 3, Key: "greeting"})
	require.NoError(t, err)
	require.True(t, reply.Success)
	require.True(t, reply.Found)
	require.Equal(t, "hello world", reply.Value)

	reply, err = client.Delete(ctx, &pb.Command{ClientId: "grpc-client", SeqNo: 4, Key: "greeting"})
	require.NoError(t, err)
	require.True(t, reply.Success)

	reply, err = client.Get(ctx, &pb.Command{ClientId: "grpc-client", SeqNo: 5, Key: "greeting"})
	require.NoError(t, err)
	require.True(t, reply.Success)
	require.False(t, reply.Found)

	// A retried mutation, byte for byte, must be answered from the dedup
	// cache rather than applied twice — over the wire just as in process.
	_, err = client.Append(ctx, &pb.Command{ClientId: "grpc-client", SeqNo: 6, Key: "counter", Value: "x"})
	require.NoError(t, err)
	_, err = client.Append(ctx, &pb.Command{ClientId: "grpc-client", SeqNo: 6, Key: "counter", Value: "x"})
	require.NoError(t, err)
	reply, err = client.Get(ctx, &pb.Command{ClientId: "grpc-client", SeqNo: 7, Key: "counter"})
	require.NoError(t, err)
	require.Equal(t, "x", reply.Value)

	_ = leaderID
}

// TestGRPCFollowerRedirects: hitting a follower over gRPC returns the
// leader hint rather than serving the request locally.
func TestGRPCFollowerRedirects(t *testing.T) {
	kc := newKVCluster(t, 3, 601)
	leaderID, _ := kc.awaitLeaderServer(t, time.Second)

	var followerID = kc.IDs[0]
	if followerID == leaderID {
		followerID = kc.IDs[1]
	}
	require.Eventually(t, func() bool {
		return kc.Nodes[followerID].Leader() == leaderID
	}, time.Second, 2*time.Millisecond)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcSrv := grpc.NewServer()
	pb.RegisterKVServer(grpcSrv, kc.Servers[followerID])
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := pb.NewKVClient(conn).Get(ctx, &pb.Command{ClientId: "grpc-client", SeqNo: 1, Key: "k"})
	require.NoError(t, err)
	require.False(t, reply.Success)
	require.Equal(t, string(leaderID), reply.LeaderHint)
}
