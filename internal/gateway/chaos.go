package gateway

// Fault injection, driven over HTTP against a real cluster. Everything
// here goes through the node's Admin service, which does to a live node
// exactly what internal/transport.Network does to a simulated one in the
// test suite: cut it off from its peers, or destroy it outright and let it
// recover from its own write-ahead log.
//
// None of it can change what is in the log. The worst a visitor can do is
// take the cluster below quorum, at which point it correctly refuses to
// accept writes until enough nodes come back — which is the single most
// worthwhile thing to be able to show.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sujalbistaa/DistKV/internal/transport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// chaosTimeout bounds an admin call. Crash and Restart both wait for
// goroutines to wind down or for a write-ahead log to be replayed, so they
// are allowed considerably longer than a status poll.
const chaosTimeout = 5 * time.Second

// Chaos applies faults to the cluster and undoes them again.
type Chaos struct {
	cluster *Cluster

	mu sync.Mutex
	// lastFaultAt is when a fault was last introduced. The watchdog uses
	// it to decide when an unattended cluster has been left broken long
	// enough to put back together.
	lastFaultAt time.Time
	healthy     bool

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewChaos returns a Chaos over cluster. If autoHeal is positive, a
// watchdog repairs any fault left in place for that long with nobody
// touching it: without one, the first visitor to partition the cluster and
// close the tab would leave it broken for everyone after them.
func NewChaos(cluster *Cluster, autoHeal time.Duration) *Chaos {
	c := &Chaos{cluster: cluster, healthy: true, stopCh: make(chan struct{})}
	if autoHeal > 0 {
		c.wg.Add(1)
		go c.watchdog(autoHeal)
	}
	return c
}

// Close stops the watchdog.
func (c *Chaos) Close() {
	c.stopOnce.Do(func() { close(c.stopCh) })
	c.wg.Wait()
}

func (c *Chaos) noteFault() {
	c.mu.Lock()
	c.lastFaultAt = time.Now()
	c.healthy = false
	c.mu.Unlock()
}

func (c *Chaos) noteHealed() {
	c.mu.Lock()
	c.healthy = true
	c.mu.Unlock()
}

func (c *Chaos) watchdog(after time.Duration) {
	defer c.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			idle := !c.healthy && time.Since(c.lastFaultAt) > after
			c.mu.Unlock()
			if !idle {
				continue
			}
			if c.clusterIsWhole() {
				c.noteHealed()
				continue
			}
			c.cluster.Record("network", "", fmt.Sprintf(
				"no activity for %s — restoring every node and healing the network", after))
			if err := c.Recover(context.Background()); err != nil {
				c.cluster.Record("network", "", "automatic recovery failed: "+err.Error())
			}
		case <-c.stopCh:
			return
		}
	}
}

// clusterIsWhole reports whether every node is running and unpartitioned.
func (c *Chaos) clusterIsWhole() bool {
	for _, node := range c.cluster.Snapshot().Nodes {
		if node.Lifecycle != "running" || len(node.IsolatedFrom) > 0 {
			return false
		}
	}
	return true
}

// Crash destroys one node.
func (c *Chaos) Crash(ctx context.Context, id transport.NodeID) error {
	admin, ok := c.cluster.admin[id]
	if !ok {
		return fmt.Errorf("unknown node %s", id)
	}
	ctx, cancel := context.WithTimeout(ctx, chaosTimeout)
	defer cancel()

	reply, err := admin.Crash(ctx, &pb.LifecycleRequest{})
	if err != nil {
		return fmt.Errorf("crashing %s: %s", id, grpcMessage(err))
	}
	if reply.Error != "" {
		return fmt.Errorf("crashing %s: %s", id, reply.Error)
	}
	c.noteFault()
	return nil
}

// CrashLeader destroys whichever node is currently leader, which is the
// interesting half of the failure space: crashing a follower proves very
// little, while crashing the leader forces a real election.
func (c *Chaos) CrashLeader(ctx context.Context) (transport.NodeID, error) {
	leader := c.cluster.Snapshot().Cluster.Leader
	if leader == "" {
		return "", fmt.Errorf("there is no leader right now — the cluster is mid-election")
	}
	id := transport.NodeID(leader)
	return id, c.Crash(ctx, id)
}

// Restart brings a crashed node back, recovering its state from disk.
func (c *Chaos) Restart(ctx context.Context, id transport.NodeID) error {
	admin, ok := c.cluster.admin[id]
	if !ok {
		return fmt.Errorf("unknown node %s", id)
	}
	ctx, cancel := context.WithTimeout(ctx, chaosTimeout)
	defer cancel()

	reply, err := admin.Restart(ctx, &pb.LifecycleRequest{})
	if err != nil {
		return fmt.Errorf("restarting %s: %s", id, grpcMessage(err))
	}
	if reply.Error != "" {
		return fmt.Errorf("restarting %s: %s", id, reply.Error)
	}
	return nil
}

// Partition splits the cluster in two: every node in group is cut off from
// every node outside it, and vice versa. Passing a single node isolates
// just that one. Passing an empty group heals everything.
//
// The isolation is set on both sides even though setting it on one would
// be enough — a node drops traffic in both directions — because a console
// showing only one side of a partition reads like a one-way fault, which
// is a meaningfully different thing.
func (c *Chaos) Partition(ctx context.Context, group []transport.NodeID) error {
	inGroup := make(map[transport.NodeID]bool, len(group))
	for _, id := range group {
		if !c.cluster.Known(id) {
			return fmt.Errorf("unknown node %s", id)
		}
		inGroup[id] = true
	}
	if len(inGroup) == len(c.cluster.Members()) {
		return fmt.Errorf("a partition with every node on one side is not a partition")
	}

	ctx, cancel := context.WithTimeout(ctx, chaosTimeout)
	defer cancel()

	var firstErr error
	for _, m := range c.cluster.Members() {
		var cutOff []string
		for _, other := range c.cluster.Members() {
			if other.ID != m.ID && inGroup[other.ID] != inGroup[m.ID] {
				cutOff = append(cutOff, string(other.ID))
			}
		}
		if _, err := c.cluster.admin[m.ID].SetPartition(ctx, &pb.SetPartitionRequest{Peers: cutOff}); err != nil {
			// A crashed node can't be told about the partition, and does
			// not need to be: it isn't talking to anyone anyway.
			if firstErr == nil && c.nodeIsRunning(m.ID) {
				firstErr = fmt.Errorf("partitioning %s: %s", m.ID, grpcMessage(err))
			}
		}
	}
	if firstErr != nil {
		return firstErr
	}
	if len(group) > 0 {
		c.noteFault()
	}
	return nil
}

// Heal removes every partition, leaving crashed nodes crashed.
func (c *Chaos) Heal(ctx context.Context) error {
	return c.Partition(ctx, nil)
}

// Recover puts the cluster back exactly as it started: every node running,
// every partition gone. It is the way out of any state a visitor can get
// the demo into.
func (c *Chaos) Recover(ctx context.Context) error {
	var firstErr error
	if err := c.Heal(ctx); err != nil {
		firstErr = err
	}
	for _, node := range c.cluster.Snapshot().Nodes {
		if node.Lifecycle == "running" {
			continue
		}
		if err := c.Restart(ctx, transport.NodeID(node.ID)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		c.noteHealed()
	}
	return firstErr
}

func (c *Chaos) nodeIsRunning(id transport.NodeID) bool {
	for _, node := range c.cluster.Snapshot().Nodes {
		if node.ID == string(id) {
			return node.Lifecycle == "running"
		}
	}
	return false
}
