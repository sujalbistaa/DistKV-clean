package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/internal/raft"
)

func TestSnapshotTrimsLog(t *testing.T) {
	dir := t.TempDir()
	d := openFresh(t, dir)

	require.NoError(t, d.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
		{Term: 2, Index: 4, Command: []byte("d")},
	}))
	require.NoError(t, d.SaveSnapshot(2, 1, []byte("state-as-of-2")))

	state, err := d.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.Log[0].Index)
	require.Equal(t, uint64(1), state.Log[0].Term)
	require.Equal(t, []byte("state-as-of-2"), state.SnapshotData)
	require.Len(t, state.Log, 3) // boundary + entries 3,4
	require.Equal(t, []byte("c"), state.Log[1].Command)
	require.Equal(t, []byte("d"), state.Log[2].Command)
}

func TestSnapshotThenAppendSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	d := openFresh(t, dir)

	require.NoError(t, d.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}))
	require.NoError(t, d.SaveSnapshot(2, 1, []byte("snap")))
	require.NoError(t, d.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 3, Command: []byte("c")},
	}))
	require.NoError(t, d.Close())

	d2 := openFresh(t, dir)
	state, err := d2.Load()
	require.NoError(t, err)
	require.Equal(t, []byte("snap"), state.SnapshotData)
	require.Len(t, state.Log, 2) // boundary + entry 3
	require.Equal(t, uint64(2), state.Log[0].Index)
	require.Equal(t, []byte("c"), state.Log[1].Command)

	// The store must still be writable after a reopen-after-compaction.
	require.NoError(t, d2.AppendLog([]raft.LogEntry{{Term: 1, Index: 4, Command: []byte("d")}}))
	state, err = d2.Load()
	require.NoError(t, err)
	require.Len(t, state.Log, 3)
	require.Equal(t, []byte("d"), state.Log[2].Command)
}

func TestSnapshotBeyondCurrentLogEmptiesIt(t *testing.T) {
	dir := t.TempDir()
	d := openFresh(t, dir)

	require.NoError(t, d.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
	}))
	// Simulates installing a leader's snapshot that covers more than this
	// follower ever had on disk.
	require.NoError(t, d.SaveSnapshot(10, 3, []byte("far-ahead")))

	state, err := d.Load()
	require.NoError(t, err)
	require.Len(t, state.Log, 1) // just the boundary
	require.Equal(t, uint64(10), state.Log[0].Index)
	require.Equal(t, uint64(3), state.Log[0].Term)

	require.NoError(t, d.AppendLog([]raft.LogEntry{{Term: 3, Index: 11, Command: []byte("next")}}))
	state, err = d.Load()
	require.NoError(t, err)
	require.Len(t, state.Log, 2)
	require.Equal(t, []byte("next"), state.Log[1].Command)
}

func TestTruncateAtOrBeforeSnapshotBoundaryErrors(t *testing.T) {
	dir := t.TempDir()
	d := openFresh(t, dir)

	require.NoError(t, d.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}))
	require.NoError(t, d.SaveSnapshot(2, 1, []byte("snap")))

	err := d.TruncateLog(2)
	require.Error(t, err)
	err = d.TruncateLog(1)
	require.Error(t, err)
}
