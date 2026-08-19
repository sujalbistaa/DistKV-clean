package shard_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/internal/kv"
	"github.com/sujalbistaa/DistKV/internal/shard"
	"github.com/sujalbistaa/DistKV/internal/testutil"
	"github.com/sujalbistaa/DistKV/internal/transport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// shardedCluster runs one independent Raft group per shard — separate fake
// networks, so the groups genuinely cannot affect one another — with a
// kv.Server on every node, plus the router and resolver a client uses to
// reach them.
type shardedCluster struct {
	shardMap *shard.Map
	router   *shard.Router
	groups   map[shard.ID]*testutil.RaftCluster
	servers  map[transport.NodeID]*kv.Server
}

// Resolve implements shard.Resolver over the in-process kv.Servers.
func (sc *shardedCluster) Resolve(node transport.NodeID) (shard.KV, error) {
	srv, ok := sc.servers[node]
	if !ok {
		return nil, fmt.Errorf("no server for node %s", node)
	}
	return srv, nil
}

// newShardedCluster builds the three-shard, three-replicas-per-shard
// topology used by the tests below: shard 1 owns keys up to "m", shard 2
// ["m","t"), shard 3 the rest.
func newShardedCluster(t *testing.T, seed int64) *shardedCluster {
	t.Helper()

	sc := &shardedCluster{
		groups:  make(map[shard.ID]*testutil.RaftCluster, 3),
		servers: make(map[transport.NodeID]*kv.Server),
	}

	bounds := []struct {
		id         shard.ID
		start, end string
	}{
		{1, "", "m"},
		{2, "m", "t"},
		{3, "t", ""},
	}

	var shards []shard.Shard
	for _, b := range bounds {
		prefix := fmt.Sprintf("s%d", b.id)
		rc := testutil.NewRaftClusterWithPrefix(t, prefix, 3, seed+int64(b.id))
		sc.groups[b.id] = rc

		for id, node := range rc.Nodes {
			srv := kv.NewServer(node, kv.NewStore())
			sc.servers[id] = srv
			t.Cleanup(srv.Stop)
		}
		shards = append(shards, shard.Shard{ID: b.id, Start: b.start, End: b.end, Replicas: rc.IDs})
	}

	sc.shardMap = &shard.Map{Shards: shards}
	require.NoError(t, sc.shardMap.Validate())
	sc.router = shard.NewRouter(sc.shardMap)
	return sc
}

func (sc *shardedCluster) client(name string) *shard.Client {
	return shard.NewClient(sc.router, sc, name)
}

// newCmd builds a raw KV command, for the few assertions that talk to one
// node's kv.Server directly instead of going through the router.
func newCmd(clientID string, seq uint64, key string) *pb.Command {
	return &pb.Command{ClientId: clientID, SeqNo: seq, Key: key}
}

// TestShardsRouteKeysIndependently: keys land in the shard that owns their
// range, and each shard's data is genuinely its own.
func TestShardsRouteKeysIndependently(t *testing.T) {
	sc := newShardedCluster(t, 700)
	for id := range sc.groups {
		sc.groups[id].AwaitLeader(t, 2*time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := sc.client("router-test")

	keys := map[string]string{"apple": "1", "mango": "2", "tomato": "3"}
	for k, v := range keys {
		require.NoError(t, c.Put(ctx, k, v))
	}
	for k, want := range keys {
		got, found, err := c.Get(ctx, k)
		require.NoError(t, err)
		require.True(t, found, "key %q", k)
		require.Equal(t, want, got, "key %q", k)
	}

	// Each key really lives in its own shard's group: shard 1's leader
	// knows "apple" but has never heard of "mango".
	shard1, err := sc.shardMap.Lookup("apple")
	require.NoError(t, err)
	leader1 := sc.groups[shard1.ID].AwaitLeader(t, 2*time.Second)
	srv := sc.servers[leader1.ID()]

	reply, err := srv.Get(ctx, newCmd("probe", 1, "mango"))
	require.NoError(t, err)
	require.True(t, reply.Success)
	require.False(t, reply.Found, "shard 1 must not hold a key that belongs to shard 2")
}

// TestShardFailureIsIsolated is the milestone 7 acceptance test: killing a
// majority of shard 2 leaves shards 1 and 3 serving normally, and shard 2
// alone becomes unavailable.
func TestShardFailureIsIsolated(t *testing.T) {
	sc := newShardedCluster(t, 701)
	for id := range sc.groups {
		sc.groups[id].AwaitLeader(t, 2*time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := sc.client("isolation-test")

	// Everything works to begin with.
	require.NoError(t, c.Put(ctx, "apple", "before"))
	require.NoError(t, c.Put(ctx, "mango", "before"))
	require.NoError(t, c.Put(ctx, "tomato", "before"))

	// Kill two of shard 2's three nodes: no majority, no leader, no
	// progress for that shard.
	shard2 := sc.groups[2]
	for _, id := range shard2.IDs[:2] {
		shard2.Network.Crash(id)
	}

	// Shards 1 and 3 keep serving reads and writes.
	require.NoError(t, c.Put(ctx, "apple", "after"))
	require.NoError(t, c.Put(ctx, "tomato", "after"))

	got, found, err := c.Get(ctx, "apple")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "after", got)

	got, found, err = c.Get(ctx, "tomato")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "after", got)

	// Shard 2 is unavailable: the request can't commit anywhere, so it
	// fails rather than returning stale data.
	shortCtx, shortCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	err = c.Put(shortCtx, "mango", "after")
	shortCancel()
	require.Error(t, err, "shard 2 lost its majority and must not accept writes")

	shortCtx, shortCancel = context.WithTimeout(ctx, 500*time.Millisecond)
	_, _, err = c.Get(shortCtx, "mango")
	shortCancel()
	require.Error(t, err, "shard 2 lost its majority and must not serve reads")

	// Shards 1 and 3 are still fine afterwards.
	require.NoError(t, c.Put(ctx, "apple", "still-serving"))
	got, _, err = c.Get(ctx, "apple")
	require.NoError(t, err)
	require.Equal(t, "still-serving", got)
}

// TestClientFollowsLeaderRedirect: the router starts knowing nothing about
// which node leads a shard, learns it from the first request, and relearns
// it after a leader change rather than getting stuck on the stale one.
func TestClientFollowsLeaderRedirect(t *testing.T) {
	sc := newShardedCluster(t, 702)
	for id := range sc.groups {
		sc.groups[id].AwaitLeader(t, 2*time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := sc.client("redirect-test")

	require.NoError(t, c.Put(ctx, "apple", "v1"))

	shard1 := sc.groups[1]
	cached, ok := sc.router.Leader(1)
	require.True(t, ok, "a successful request should have populated the leader cache")

	// Crash the leader the client just learned; its cached entry is now
	// wrong and must be replaced by whoever wins the next election.
	shard1.Network.Crash(cached)
	require.NoError(t, c.Put(ctx, "apple", "v2"))

	newLeader, ok := sc.router.Leader(1)
	require.True(t, ok)
	require.NotEqual(t, cached, newLeader, "the router should have relearned the leader")

	got, found, err := c.Get(ctx, "apple")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "v2", got)
}
