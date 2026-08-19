package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/storage"
)

func openFresh(t *testing.T, dir string) *storage.DiskStorage {
	t.Helper()
	d, err := storage.NewDiskStorage(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestHardStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	d := openFresh(t, dir)

	require.NoError(t, d.SaveHardState(7, "node-2"))
	state, err := d.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(7), state.CurrentTerm)
	require.EqualValues(t, "node-2", state.VotedFor)
	require.Equal(t, []raft.LogEntry{{Term: 0, Index: 0}}, state.Log)
}

func TestAppendLogRoundTrip(t *testing.T) {
	dir := t.TempDir()
	d := openFresh(t, dir)

	entries := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("bb")},
		{Term: 2, Index: 3, Command: nil},
	}
	require.NoError(t, d.AppendLog(entries))

	state, err := d.Load()
	require.NoError(t, err)
	require.Len(t, state.Log, 4) // sentinel + 3
	require.Equal(t, raft.LogEntry{Term: 0, Index: 0}, state.Log[0])
	require.Equal(t, entries, state.Log[1:])
}

func TestTruncateThenAppendOverwrites(t *testing.T) {
	dir := t.TempDir()
	d := openFresh(t, dir)

	require.NoError(t, d.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("x")},
		{Term: 1, Index: 2, Command: []byte("y")},
		{Term: 1, Index: 3, Command: []byte("z")},
	}))
	require.NoError(t, d.TruncateLog(2))
	require.NoError(t, d.AppendLog([]raft.LogEntry{
		{Term: 2, Index: 2, Command: []byte("Y")},
	}))

	state, err := d.Load()
	require.NoError(t, err)
	require.Len(t, state.Log, 3) // sentinel + 2
	require.Equal(t, []byte("x"), state.Log[1].Command)
	require.Equal(t, []byte("Y"), state.Log[2].Command)
	require.Equal(t, uint64(2), state.Log[2].Term)
}

// TestReopenAfterClose simulates a clean process restart: a fresh
// DiskStorage pointed at the same directory must see everything the first
// one wrote.
func TestReopenAfterClose(t *testing.T) {
	dir := t.TempDir()
	d1 := openFresh(t, dir)
	require.NoError(t, d1.SaveHardState(3, "node-1"))
	require.NoError(t, d1.AppendLog([]raft.LogEntry{{Term: 3, Index: 1, Command: []byte("v")}}))
	require.NoError(t, d1.Close())

	d2 := openFresh(t, dir)
	state, err := d2.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(3), state.CurrentTerm)
	require.EqualValues(t, "node-1", state.VotedFor)
	require.Len(t, state.Log, 2)
	require.Equal(t, []byte("v"), state.Log[1].Command)
}

// TestTornTrailingRecordIsTruncatedOnLoad simulates a crash mid-fsync: the
// last record's header claims more payload bytes than were actually
// written. Load must detect this, discard the torn record, keep everything
// before it, and leave the file physically truncated so future appends
// start clean.
func TestTornTrailingRecordIsTruncatedOnLoad(t *testing.T) {
	dir := t.TempDir()
	d := openFresh(t, dir)
	require.NoError(t, d.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("good")},
	}))
	require.NoError(t, d.Close())

	walPath := filepath.Join(dir, "log.wal")
	goodSize := fileSize(t, walPath)

	// Append a header claiming a large payload, followed by only a few
	// bytes of it -- exactly what a crash mid-write to the WAL leaves
	// behind.
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0, 0, 1, 0, 0xDE, 0xAD, 0xBE, 0xEF, 1, 2, 3})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	d2 := openFresh(t, dir)
	state, err := d2.Load()
	require.NoError(t, err)
	require.Len(t, state.Log, 2) // sentinel + the one good record
	require.Equal(t, []byte("good"), state.Log[1].Command)

	require.Equal(t, goodSize, fileSize(t, walPath), "torn tail should have been truncated away")

	// The repaired store must still be writable and produce a consistent
	// file: appending after repair should not corrupt or reintroduce the
	// torn bytes.
	require.NoError(t, d2.AppendLog([]raft.LogEntry{{Term: 1, Index: 2, Command: []byte("next")}}))
	state, err = d2.Load()
	require.NoError(t, err)
	require.Len(t, state.Log, 3)
	require.Equal(t, []byte("next"), state.Log[2].Command)
}

// TestCorruptRecordChecksumIsTruncatedOnLoad flips a byte inside an
// otherwise well-formed record so its CRC32 no longer matches, simulating
// on-disk corruption rather than a torn write, and expects the same
// self-healing behaviour.
func TestCorruptRecordChecksumIsTruncatedOnLoad(t *testing.T) {
	dir := t.TempDir()
	d := openFresh(t, dir)
	require.NoError(t, d.AppendLog([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("first")},
		{Term: 1, Index: 2, Command: []byte("second")},
	}))
	require.NoError(t, d.Close())

	walPath := filepath.Join(dir, "log.wal")
	raw, err := os.ReadFile(walPath)
	require.NoError(t, err)
	// Flip a byte inside the second record's payload (well past the first
	// record's header+payload).
	firstRecordLen := int64(8 + 8 + 8 + 4 + len("first"))
	corruptAt := firstRecordLen + 10
	require.Less(t, corruptAt, int64(len(raw)))
	raw[corruptAt] ^= 0xFF
	require.NoError(t, os.WriteFile(walPath, raw, 0o644))

	d2 := openFresh(t, dir)
	state, err := d2.Load()
	require.NoError(t, err)
	require.Len(t, state.Log, 2) // sentinel + only the first, valid record
	require.Equal(t, []byte("first"), state.Log[1].Command)
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Size()
}
