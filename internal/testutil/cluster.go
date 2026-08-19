// Package testutil provides the in-process cluster harness used by tests
// throughout DistKV: a fake, fault-injecting network shared by every node,
// with helpers to partition, disconnect, and crash nodes deterministically.
package testutil

// Locking: Cluster's own fields (IDs, Trans) are populated once in
// NewCluster and never mutated afterward, so no lock is needed for them.
// The mutable, concurrency-sensitive state lives in transport.Network,
// which guards itself.

import (
	"context"
	"fmt"
	"testing"

	"github.com/sujalbistaa/DistKV/internal/transport"
)

// Cluster is a set of in-process nodes sharing one fake transport.Network.
type Cluster struct {
	Network *transport.Network
	IDs     []transport.NodeID
	Trans   map[transport.NodeID]transport.Transport
}

// NewCluster creates n nodes named node-0..node-(n-1) sharing a single fake
// Network seeded with seed, and closes their transports on test cleanup.
func NewCluster(t testing.TB, n int, seed int64) *Cluster {
	t.Helper()
	return NewClusterWithPrefix(t, "node", n, seed)
}

// NewClusterWithPrefix is NewCluster with control over node naming, for
// tests that run several independent clusters (one per shard, say) and
// need their node IDs to be distinct from each other.
func NewClusterWithPrefix(t testing.TB, prefix string, n int, seed int64) *Cluster {
	t.Helper()
	net := transport.NewNetwork(seed)
	c := &Cluster{
		Network: net,
		Trans:   make(map[transport.NodeID]transport.Transport, n),
	}
	for i := 0; i < n; i++ {
		id := transport.NodeID(fmt.Sprintf("%s-%d", prefix, i))
		c.IDs = append(c.IDs, id)
		c.Trans[id] = net.NewTransport(id)
	}
	t.Cleanup(func() {
		for _, tr := range c.Trans {
			_ = tr.Close()
		}
	})
	return c
}

// Partition splits the cluster into two groups by NodeID.
func (c *Cluster) Partition(groupA, groupB []transport.NodeID) {
	c.Network.Partition(groupA, groupB)
}

// Heal removes any active partition.
func (c *Cluster) Heal() {
	c.Network.Heal()
}

// Send issues req from one cluster node to another through the shared fake
// network and returns the destination handler's reply.
func (c *Cluster) Send(ctx context.Context, from, to transport.NodeID, req any) (any, error) {
	return c.Trans[from].Send(ctx, to, req)
}

// Majority splits IDs into two groups of size >= 1 and <= len(IDs)-1: the
// first `size` IDs and the rest. Useful for constructing majority/minority
// partitions in tests.
func (c *Cluster) Split(size int) (a, b []transport.NodeID) {
	return c.IDs[:size], c.IDs[size:]
}
