package shard

// Locking: mu guards leaders. The Map itself is immutable after Validate
// (shard assignment is static), so it needs no lock.

import (
	"sync"

	"github.com/sujalbistaa/DistKV/internal/transport"
)

// Router resolves a key to its shard and that shard's current leader. The
// leader is a cache, not a source of truth: it's learned from successful
// requests and from redirect hints, and invalidated whenever a node turns
// out not to be leader after all. On a cache miss the caller falls back to
// trying the shard's replicas in order, which is how the cache gets
// (re)populated after an election.
type Router struct {
	shards *Map

	mu      sync.Mutex
	leaders map[ID]transport.NodeID
}

// NewRouter returns a Router over an already-validated shard map.
func NewRouter(m *Map) *Router {
	return &Router{
		shards:  m,
		leaders: make(map[ID]transport.NodeID),
	}
}

// Map returns the router's shard map.
func (r *Router) Map() *Map { return r.shards }

// ShardFor returns the shard responsible for key.
func (r *Router) ShardFor(key string) (Shard, error) {
	return r.shards.Lookup(key)
}

// Leader returns the cached leader for a shard, if one is known.
func (r *Router) Leader(id ID) (transport.NodeID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	leader, ok := r.leaders[id]
	return leader, ok
}

// SetLeader records a shard's leader, either because a request to it
// succeeded or because another node redirected us there.
func (r *Router) SetLeader(id ID, leader transport.NodeID) {
	if leader == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leaders[id] = leader
}

// InvalidateLeader forgets a shard's cached leader, after a request to it
// was refused or failed.
func (r *Router) InvalidateLeader(id ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.leaders, id)
}

// Candidates returns the order in which a client should try a shard's
// nodes: the cached leader first, if any, then the remaining replicas.
func (r *Router) Candidates(s Shard) []transport.NodeID {
	cached, ok := r.Leader(s.ID)
	if !ok {
		return append([]transport.NodeID(nil), s.Replicas...)
	}
	candidates := make([]transport.NodeID, 0, len(s.Replicas))
	candidates = append(candidates, cached)
	for _, node := range s.Replicas {
		if node != cached {
			candidates = append(candidates, node)
		}
	}
	return candidates
}
