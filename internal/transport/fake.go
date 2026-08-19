package transport

// Locking: mu guards every field of Network below. The RNG is only ever
// touched while mu is held, since *rand.Rand is not safe for concurrent use.
// Handlers are invoked without holding mu, so a handler that calls back into
// Send (directly or via another goroutine) cannot deadlock against it.

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// Network is a shared in-memory simulation of the links between a set of
// nodes. Multiple Transport values, one per node, are created from a single
// Network so that a test can control how they see each other: dropped,
// delayed, duplicated or reordered messages, disconnects, partitions and
// crashes. All randomness is drawn from a seeded RNG so a failing test
// reproduces exactly given the same seed.
type Network struct {
	mu  sync.Mutex
	rng *rand.Rand

	minLatency, maxLatency time.Duration
	dropRate               float64
	duplicateRate          float64

	disconnected map[NodeID]bool
	crashed      map[NodeID]bool
	partitioned  bool
	group        map[NodeID]int

	handlers map[NodeID]Handler
}

// NewNetwork creates a Network whose fault injection is driven by the given
// seed. The same seed always produces the same sequence of drop/duplicate/
// latency decisions for a given sequence of Send calls.
func NewNetwork(seed int64) *Network {
	return &Network{
		rng:          rand.New(rand.NewSource(seed)),
		disconnected: make(map[NodeID]bool),
		crashed:      make(map[NodeID]bool),
		group:        make(map[NodeID]int),
		handlers:     make(map[NodeID]Handler),
		minLatency:   time.Millisecond,
		maxLatency:   time.Millisecond,
	}
}

// NewTransport returns a Transport bound to id, backed by this Network.
func (n *Network) NewTransport(id NodeID) Transport {
	return &fakeTransport{id: id, net: n}
}

// Disconnect makes node unreachable in both directions without affecting
// other nodes.
func (n *Network) Disconnect(node NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.disconnected[node] = true
}

// Reconnect undoes a prior Disconnect.
func (n *Network) Reconnect(node NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.disconnected, node)
}

// Crash marks node as crashed: it can neither send nor receive until
// Restart is called. Unlike Disconnect, a crashed node's peers cannot reach
// it even if the node itself later calls Send (mirroring a stopped process).
func (n *Network) Crash(node NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.crashed[node] = true
}

// Restart undoes a prior Crash.
func (n *Network) Restart(node NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.crashed, node)
}

// SetLatency sets the range from which per-message delivery latency is drawn
// uniformly. min must be <= max.
func (n *Network) SetLatency(min, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.minLatency, n.maxLatency = min, max
}

// SetDropRate sets the probability, in [0,1], that a given message is
// dropped in flight.
func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = p
}

// SetDuplicateRate sets the probability, in [0,1], that a given message that
// was not dropped is also delivered a second time to the receiver's
// handler. The sender still observes exactly one reply.
func (n *Network) SetDuplicateRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.duplicateRate = p
}

// Partition splits the network into two groups. Nodes within a group can
// reach each other; nodes in different groups cannot. A node named in
// neither slice cannot reach, or be reached by, either group.
func (n *Network) Partition(groupA, groupB []NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = make(map[NodeID]int, len(groupA)+len(groupB))
	for _, id := range groupA {
		n.group[id] = 1
	}
	for _, id := range groupB {
		n.group[id] = 2
	}
	n.partitioned = true
}

// Heal removes any active partition. Individual Disconnects and Crashes are
// unaffected.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned = false
	n.group = make(map[NodeID]int)
}

func (n *Network) reachableLocked(a, b NodeID) bool {
	if n.disconnected[a] || n.disconnected[b] {
		return false
	}
	if n.partitioned && n.group[a] != n.group[b] {
		return false
	}
	return true
}

func (n *Network) randomLatencyLocked() time.Duration {
	if n.maxLatency <= n.minLatency {
		return n.minLatency
	}
	span := int64(n.maxLatency - n.minLatency)
	return n.minLatency + time.Duration(n.rng.Int63n(span))
}

func (n *Network) randomLatency() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.randomLatencyLocked()
}

type fakeTransport struct {
	id  NodeID
	net *Network
}

func (t *fakeTransport) LocalID() NodeID { return t.id }

func (t *fakeTransport) RegisterHandler(h Handler) {
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	t.net.handlers[t.id] = h
}

func (t *fakeTransport) Close() error {
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	delete(t.net.handlers, t.id)
	return nil
}

// Send routes req through the shared Network, applying whatever fault
// injection is currently configured, then invokes the destination's
// registered handler and returns its reply.
func (t *fakeTransport) Send(ctx context.Context, to NodeID, req any) (any, error) {
	n := t.net

	n.mu.Lock()
	if n.crashed[t.id] || n.crashed[to] {
		n.mu.Unlock()
		return nil, ErrCrashed
	}
	if !n.reachableLocked(t.id, to) {
		n.mu.Unlock()
		return nil, ErrUnreachable
	}
	dropped := n.rng.Float64() < n.dropRate
	duplicate := !dropped && n.rng.Float64() < n.duplicateRate
	latency := n.randomLatencyLocked()
	handler, ok := n.handlers[to]
	n.mu.Unlock()

	if err := waitLatency(ctx, latency); err != nil {
		return nil, err
	}
	if dropped {
		return nil, ErrDropped
	}
	if !ok {
		return nil, ErrNoHandler
	}

	if duplicate {
		// Model a network that delivered the packet twice: the receiver's
		// handler runs a second time, independently delayed, but the
		// sender only ever sees the reply from the primary delivery below.
		go func() {
			if waitLatency(context.Background(), n.randomLatency()) == nil {
				_, _ = handler(context.Background(), t.id, req)
			}
		}()
	}

	return handler(ctx, t.id, req)
}

func waitLatency(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
