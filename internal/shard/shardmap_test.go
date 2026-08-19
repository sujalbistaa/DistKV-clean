package shard_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/internal/shard"
	"github.com/sujalbistaa/DistKV/internal/transport"
)

func threeShardMap() *shard.Map {
	return &shard.Map{Shards: []shard.Shard{
		{ID: 1, Start: "", End: "m", Replicas: []transport.NodeID{"s1-0", "s1-1", "s1-2"}},
		{ID: 2, Start: "m", End: "t", Replicas: []transport.NodeID{"s2-0", "s2-1", "s2-2"}},
		{ID: 3, Start: "t", End: "", Replicas: []transport.NodeID{"s3-0", "s3-1", "s3-2"}},
	}}
}

func TestMapLookupRoutesKeysToRanges(t *testing.T) {
	m := threeShardMap()
	require.NoError(t, m.Validate())

	for key, want := range map[string]shard.ID{
		"":       1,
		"apple":  1,
		"lemon":  1,
		"m":      2,
		"mango":  2,
		"sugar":  2,
		"t":      3,
		"tomato": 3,
		"zebra":  3,
	} {
		got, err := m.Lookup(key)
		require.NoError(t, err, "key %q", key)
		require.Equal(t, want, got.ID, "key %q", key)
	}
}

func TestMapValidateAcceptsUnsortedInput(t *testing.T) {
	m := &shard.Map{Shards: []shard.Shard{
		{ID: 3, Start: "t", End: "", Replicas: []transport.NodeID{"c"}},
		{ID: 1, Start: "", End: "m", Replicas: []transport.NodeID{"a"}},
		{ID: 2, Start: "m", End: "t", Replicas: []transport.NodeID{"b"}},
	}}
	require.NoError(t, m.Validate())
	require.Equal(t, shard.ID(1), m.Shards[0].ID, "Validate should sort shards by range start")
}

func TestMapValidateRejectsBadConfigs(t *testing.T) {
	cases := map[string]*shard.Map{
		"no shards": {},
		"gap in keyspace": {Shards: []shard.Shard{
			{ID: 1, Start: "", End: "m", Replicas: []transport.NodeID{"a"}},
			{ID: 2, Start: "p", End: "", Replicas: []transport.NodeID{"b"}},
		}},
		"overlapping ranges": {Shards: []shard.Shard{
			{ID: 1, Start: "", End: "p", Replicas: []transport.NodeID{"a"}},
			{ID: 2, Start: "m", End: "", Replicas: []transport.NodeID{"b"}},
		}},
		"does not start at beginning": {Shards: []shard.Shard{
			{ID: 1, Start: "a", End: "", Replicas: []transport.NodeID{"a"}},
		}},
		"does not reach end": {Shards: []shard.Shard{
			{ID: 1, Start: "", End: "m", Replicas: []transport.NodeID{"a"}},
		}},
		"duplicate ids": {Shards: []shard.Shard{
			{ID: 1, Start: "", End: "m", Replicas: []transport.NodeID{"a"}},
			{ID: 1, Start: "m", End: "", Replicas: []transport.NodeID{"b"}},
		}},
		"shard without replicas": {Shards: []shard.Shard{
			{ID: 1, Start: "", End: "m", Replicas: []transport.NodeID{"a"}},
			{ID: 2, Start: "m", End: "", Replicas: nil},
		}},
		"inverted range": {Shards: []shard.Shard{
			{ID: 1, Start: "", End: "z", Replicas: []transport.NodeID{"a"}},
			{ID: 2, Start: "z", End: "b", Replicas: []transport.NodeID{"b"}},
		}},
	}

	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			require.Error(t, m.Validate())
		})
	}
}

func TestLoadMapFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shards.json")
	config := `{
	  "shards": [
	    {"id": 1, "start": "",  "end": "m", "replicas": ["s1-0", "s1-1", "s1-2"]},
	    {"id": 2, "start": "m", "end": "t", "replicas": ["s2-0", "s2-1", "s2-2"]},
	    {"id": 3, "start": "t", "end": "",  "replicas": ["s3-0", "s3-1", "s3-2"]}
	  ]
	}`
	require.NoError(t, os.WriteFile(path, []byte(config), 0o644))

	m, err := shard.LoadMap(path)
	require.NoError(t, err)
	require.Len(t, m.Shards, 3)

	s, err := m.Lookup("mango")
	require.NoError(t, err)
	require.Equal(t, shard.ID(2), s.ID)
	require.Equal(t, []transport.NodeID{"s2-0", "s2-1", "s2-2"}, s.Replicas)
}

func TestLoadMapRejectsInvalidFile(t *testing.T) {
	dir := t.TempDir()

	badJSON := filepath.Join(dir, "bad.json")
	require.NoError(t, os.WriteFile(badJSON, []byte("{not json"), 0o644))
	_, err := shard.LoadMap(badJSON)
	require.Error(t, err)

	gappy := filepath.Join(dir, "gap.json")
	require.NoError(t, os.WriteFile(gappy, []byte(`{"shards":[{"id":1,"start":"","end":"m","replicas":["a"]}]}`), 0o644))
	_, err = shard.LoadMap(gappy)
	require.Error(t, err)

	_, err = shard.LoadMap(filepath.Join(dir, "does-not-exist.json"))
	require.Error(t, err)
}

func TestRouterCachesAndInvalidatesLeader(t *testing.T) {
	m := threeShardMap()
	require.NoError(t, m.Validate())
	r := shard.NewRouter(m)

	s, err := r.ShardFor("mango")
	require.NoError(t, err)
	require.Equal(t, shard.ID(2), s.ID)

	// With nothing cached, candidates are just the replicas in order.
	require.Equal(t, s.Replicas, r.Candidates(s))
	_, ok := r.Leader(s.ID)
	require.False(t, ok)

	r.SetLeader(s.ID, "s2-2")
	leader, ok := r.Leader(s.ID)
	require.True(t, ok)
	require.EqualValues(t, "s2-2", leader)

	// The cached leader is tried first, and no replica appears twice.
	require.Equal(t, []transport.NodeID{"s2-2", "s2-0", "s2-1"}, r.Candidates(s))

	r.InvalidateLeader(s.ID)
	_, ok = r.Leader(s.ID)
	require.False(t, ok)
	require.Equal(t, s.Replicas, r.Candidates(s))
}
