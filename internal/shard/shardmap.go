// Package shard implements static, range-partitioned sharding: a shard map
// loaded from config, and a router that resolves a key to its shard and
// that shard's current leader. Each shard is an independent Raft group, so
// losing a majority in one shard leaves the others serving.
//
// There is deliberately no live migration or rebalancing: shard
// assignment is fixed at config load. See docs/design.md.
package shard

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/sujalbistaa/DistKV/internal/transport"
)

// ID identifies a shard.
type ID int

// Shard is one range of the keyspace, [Start, End), served by its own Raft
// group. An empty Start means the range is unbounded below, and an empty
// End means unbounded above.
type Shard struct {
	ID       ID                 `json:"id"`
	Start    string             `json:"start"`
	End      string             `json:"end"`
	Replicas []transport.NodeID `json:"replicas"`
}

// Contains reports whether key falls in this shard's range.
func (s Shard) Contains(key string) bool {
	if key < s.Start {
		return false
	}
	if s.End == "" {
		return true
	}
	return key < s.End
}

// Map is a complete, validated partitioning of the keyspace.
type Map struct {
	Shards []Shard `json:"shards"`
}

// LoadMap reads and validates a shard map from a JSON config file.
func LoadMap(path string) (*Map, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("shard: reading config %s: %w", path, err)
	}
	var m Map
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("shard: parsing config %s: %w", path, err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("shard: config %s: %w", path, err)
	}
	return &m, nil
}

// Validate checks that the shards form a contiguous, non-overlapping
// partition of the whole keyspace, and that each has a distinct ID and at
// least one replica. It sorts Shards by Start as a side effect, so Lookup
// can rely on the ordering.
func (m *Map) Validate() error {
	if len(m.Shards) == 0 {
		return fmt.Errorf("no shards defined")
	}

	sort.Slice(m.Shards, func(i, j int) bool { return m.Shards[i].Start < m.Shards[j].Start })

	seen := make(map[ID]bool, len(m.Shards))
	for i, s := range m.Shards {
		if seen[s.ID] {
			return fmt.Errorf("duplicate shard id %d", s.ID)
		}
		seen[s.ID] = true

		if len(s.Replicas) == 0 {
			return fmt.Errorf("shard %d has no replicas", s.ID)
		}
		if s.End != "" && s.Start >= s.End {
			return fmt.Errorf("shard %d has an empty or inverted range [%q, %q)", s.ID, s.Start, s.End)
		}

		switch {
		case i == 0 && s.Start != "":
			return fmt.Errorf("shard %d is the lowest shard but does not start at the beginning of the keyspace", s.ID)
		case i == len(m.Shards)-1 && s.End != "":
			return fmt.Errorf("shard %d is the highest shard but does not extend to the end of the keyspace", s.ID)
		case i > 0 && m.Shards[i-1].End != s.Start:
			return fmt.Errorf("gap or overlap between shard %d (ends %q) and shard %d (starts %q)",
				m.Shards[i-1].ID, m.Shards[i-1].End, s.ID, s.Start)
		}
	}
	return nil
}

// Lookup returns the shard responsible for key. A validated Map covers the
// entire keyspace, so this always finds one.
func (m *Map) Lookup(key string) (Shard, error) {
	for _, s := range m.Shards {
		if s.Contains(key) {
			return s, nil
		}
	}
	return Shard{}, fmt.Errorf("shard: no shard covers key %q (shard map is not a complete partition)", key)
}

// Shard returns the shard with the given ID.
func (m *Map) Shard(id ID) (Shard, error) {
	for _, s := range m.Shards {
		if s.ID == id {
			return s, nil
		}
	}
	return Shard{}, fmt.Errorf("shard: no shard with id %d", id)
}
