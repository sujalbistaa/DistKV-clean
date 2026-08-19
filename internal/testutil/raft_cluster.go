package testutil

// Locking: RaftCluster itself holds no mutable state beyond the Nodes map,
// which is populated once in NewRaftCluster and never mutated afterward.
// Each raft.Node guards its own state.

import (
	"testing"
	"time"

	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/transport"
)

// RaftCluster is a Cluster with a raft.Node wired up on every node, used by
// every milestone from leader election onward.
type RaftCluster struct {
	*Cluster
	Nodes map[transport.NodeID]*raft.Node

	cfgs map[transport.NodeID]raft.Config
}

// RaftClusterOpt customizes the raft.Config for node i (0-indexed, in the
// order Cluster.IDs lists nodes) before it is constructed.
type RaftClusterOpt func(i int, cfg *raft.Config)

// NewRaftCluster builds n in-process Raft nodes sharing one fake network.
// Election and heartbeat timings default to short, test-friendly values so
// suites run quickly; override them with opts if a test needs otherwise.
// Every node is stopped automatically on test cleanup.
func NewRaftCluster(t testing.TB, n int, seed int64, opts ...RaftClusterOpt) *RaftCluster {
	t.Helper()
	return NewRaftClusterWithPrefix(t, "node", n, seed, opts...)
}

// NewRaftClusterWithPrefix is NewRaftCluster with control over node
// naming, for tests running several independent groups at once (one per
// shard, say) that need globally distinct node IDs.
func NewRaftClusterWithPrefix(t testing.TB, prefix string, n int, seed int64, opts ...RaftClusterOpt) *RaftCluster {
	t.Helper()
	c := NewClusterWithPrefix(t, prefix, n, seed)
	rc := &RaftCluster{
		Cluster: c,
		Nodes:   make(map[transport.NodeID]*raft.Node, n),
		cfgs:    make(map[transport.NodeID]raft.Config, n),
	}

	for i, id := range c.IDs {
		var peers []transport.NodeID
		for _, other := range c.IDs {
			if other != id {
				peers = append(peers, other)
			}
		}
		cfg := raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          c.Trans[id],
			ElectionTimeoutMin: 30 * time.Millisecond,
			ElectionTimeoutMax: 60 * time.Millisecond,
			HeartbeatInterval:  10 * time.Millisecond,
			TickInterval:       3 * time.Millisecond,
			RPCTimeout:         15 * time.Millisecond,
			Seed:               seed + int64(i) + 1,
		}
		for _, opt := range opts {
			opt(i, &cfg)
		}
		node, err := raft.NewNode(cfg)
		if err != nil {
			t.Fatalf("raft.NewNode(%s): %v", id, err)
		}
		rc.Nodes[id] = node
		rc.cfgs[id] = cfg
	}

	t.Cleanup(func() {
		for _, node := range rc.Nodes {
			node.Stop()
		}
	})

	return rc
}

// Restart simulates a real process crash and restart: it stops the current
// *raft.Node for id (discarding all its in-memory state) and constructs a
// fresh one from the same Config, including the same Storage and
// Transport — so a caller using a real DiskStorage (rather than the
// zero-value Config's default MemoryStorage) sees genuine recovery from
// disk, not just a resumed in-memory object.
func (rc *RaftCluster) Restart(t testing.TB, id transport.NodeID) *raft.Node {
	t.Helper()
	rc.Nodes[id].Stop()
	node, err := raft.NewNode(rc.cfgs[id])
	if err != nil {
		t.Fatalf("raft.NewNode(%s) on restart: %v", id, err)
	}
	rc.Nodes[id] = node
	return node
}

// AwaitLeader polls until exactly one node reports Leader role and returns
// it, failing the test if that doesn't happen within timeout or if more
// than one node ever reports Leader simultaneously.
func (rc *RaftCluster) AwaitLeader(t testing.TB, timeout time.Duration) *raft.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*raft.Node
		for _, node := range rc.Nodes {
			if _, role := node.State(); role == raft.Leader {
				leaders = append(leaders, node)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		if len(leaders) > 1 {
			t.Fatalf("observed %d leaders simultaneously", len(leaders))
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}
