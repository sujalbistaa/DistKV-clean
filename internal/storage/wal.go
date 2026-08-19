// Package storage implements DiskStorage, the disk-backed raft.Storage used
// in production: a small hardstate file for currentTerm/votedFor, rewritten
// atomically via write-temp-then-rename, and an append-only, checksummed
// write-ahead log of entries.
package storage

// Locking: mu serializes every access to the open WAL file handle and the
// in-memory offsets index, for the whole duration of each exported method
// including its fsync — Raft depends on SaveHardState/AppendLog/TruncateLog
// being linearizable with respect to each other and with Load.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/transport"
)

const (
	hardStateFileName = "hardstate"
	walFileName       = "log.wal"
	snapshotFileName  = "snapshot"

	// recordHeaderSize is [4-byte payload length][4-byte CRC32].
	recordHeaderSize = 8
	// maxRecordPayload bounds the allocation Load() will attempt for a
	// single record, so a garbage length field from a corrupted file can't
	// force an unbounded allocation; it is treated as corruption instead.
	maxRecordPayload = 64 << 20
)

// DiskStorage is a raft.Storage backed by files under dir. Every method
// that mutates state fsyncs before returning, so a caller that has been
// told a call succeeded can rely on it surviving a crash immediately after.
type DiskStorage struct {
	mu sync.Mutex

	dir string
	wal *os.File
	// offsets[i] = byte offset of the record for absolute log index
	// lastIncludedIndex+i+1; both are reset together whenever a snapshot
	// is saved, since that's when the WAL is compacted.
	offsets           []int64
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
}

// NewDiskStorage opens (creating if necessary) the write-ahead log and
// hardstate files under dir.
func NewDiskStorage(dir string) (*DiskStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: creating data dir %s: %w", dir, err)
	}
	wal, err := os.OpenFile(filepath.Join(dir, walFileName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: opening wal: %w", err)
	}
	return &DiskStorage{dir: dir, wal: wal}, nil
}

// Close releases the underlying file handles.
func (d *DiskStorage) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.wal.Close()
}

// SaveHardState durably persists currentTerm and votedFor via
// write-to-temp-file, fsync, then atomic rename, so a crash mid-write
// leaves the previous hardstate file intact rather than a torn one.
func (d *DiskStorage) SaveHardState(term uint64, votedFor transport.NodeID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	buf := make([]byte, 8+4+len(votedFor))
	binary.BigEndian.PutUint64(buf[0:8], term)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(votedFor)))
	copy(buf[12:], votedFor)

	tmpPath := filepath.Join(d.dir, hardStateFileName+".tmp")
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("storage: creating hardstate temp file: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("storage: writing hardstate: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("storage: fsyncing hardstate: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("storage: closing hardstate temp file: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(d.dir, hardStateFileName)); err != nil {
		return fmt.Errorf("storage: installing hardstate: %w", err)
	}
	return nil
}

// AppendLog durably appends entries to the end of the write-ahead log.
func (d *DiskStorage) AppendLog(entries []raft.LogEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, e := range entries {
		offset, err := d.wal.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("storage: seeking wal: %w", err)
		}
		payload := encodeEntry(e)
		header := make([]byte, recordHeaderSize)
		binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
		if _, err := d.wal.Write(header); err != nil {
			return fmt.Errorf("storage: writing wal record header: %w", err)
		}
		if _, err := d.wal.Write(payload); err != nil {
			return fmt.Errorf("storage: writing wal record: %w", err)
		}
		d.offsets = append(d.offsets, offset)
	}
	if err := d.wal.Sync(); err != nil {
		return fmt.Errorf("storage: fsyncing wal: %w", err)
	}
	return nil
}

// TruncateLog durably discards every persisted entry at index >= fromIndex,
// which must be strictly greater than the current snapshot's
// lastIncludedIndex.
func (d *DiskStorage) TruncateLog(fromIndex uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if fromIndex <= d.lastIncludedIndex {
		return fmt.Errorf("storage: TruncateLog: fromIndex %d is at or before the snapshot boundary %d", fromIndex, d.lastIncludedIndex)
	}
	pos := fromIndex - d.lastIncludedIndex - 1
	if int(pos) >= len(d.offsets) {
		return nil
	}
	offset := d.offsets[pos]
	if err := d.wal.Truncate(offset); err != nil {
		return fmt.Errorf("storage: truncating wal: %w", err)
	}
	if _, err := d.wal.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("storage: seeking wal after truncate: %w", err)
	}
	if err := d.wal.Sync(); err != nil {
		return fmt.Errorf("storage: fsyncing wal after truncate: %w", err)
	}
	d.offsets = d.offsets[:pos]
	return nil
}

// SaveSnapshot durably persists data as the application's state as of
// lastIncludedIndex/lastIncludedTerm (write-temp-fsync-rename, like
// SaveHardState), then compacts the WAL down to only the entries after
// lastIncludedIndex.
func (d *DiskStorage) SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.writeSnapshotFileLocked(lastIncludedIndex, lastIncludedTerm, data); err != nil {
		return err
	}
	return d.compactWALLocked(lastIncludedIndex, lastIncludedTerm)
}

func (d *DiskStorage) writeSnapshotFileLocked(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	buf := make([]byte, 8+8+4+len(data))
	binary.BigEndian.PutUint64(buf[0:8], lastIncludedIndex)
	binary.BigEndian.PutUint64(buf[8:16], lastIncludedTerm)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(data)))
	copy(buf[20:], data)

	tmpPath := filepath.Join(d.dir, snapshotFileName+".tmp")
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("storage: creating snapshot temp file: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("storage: writing snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("storage: fsyncing snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("storage: closing snapshot temp file: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(d.dir, snapshotFileName)); err != nil {
		return fmt.Errorf("storage: installing snapshot: %w", err)
	}
	return nil
}

// compactWALLocked rewrites the WAL to contain only the entries after
// lastIncludedIndex, via write-to-temp-then-rename so a crash mid-compact
// leaves the previous, still-valid (if larger) WAL in place. Callers
// (Node.Snapshot and handleInstallSnapshot) only ever call SaveSnapshot
// with a lastIncludedIndex strictly greater than what's already saved.
func (d *DiskStorage) compactWALLocked(lastIncludedIndex, lastIncludedTerm uint64) error {
	relPos := int(lastIncludedIndex - d.lastIncludedIndex)

	var cutPoint int64
	var newOffsets []int64
	if relPos < len(d.offsets) {
		cutPoint = d.offsets[relPos]
		newOffsets = make([]int64, 0, len(d.offsets)-relPos)
		for _, off := range d.offsets[relPos:] {
			newOffsets = append(newOffsets, off-cutPoint)
		}
	} else {
		// lastIncludedIndex is at or beyond everything currently on disk;
		// nothing survives.
		end, err := d.wal.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("storage: seeking wal: %w", err)
		}
		cutPoint = end
	}

	tmpPath := filepath.Join(d.dir, walFileName+".compact")
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("storage: creating compacted wal: %w", err)
	}
	if _, err := d.wal.Seek(cutPoint, io.SeekStart); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: seeking source wal: %w", err)
	}
	if _, err := io.Copy(tmp, d.wal); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: copying surviving wal entries: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("storage: fsyncing compacted wal: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("storage: closing compacted wal: %w", err)
	}

	walPath := filepath.Join(d.dir, walFileName)
	if err := os.Rename(tmpPath, walPath); err != nil {
		return fmt.Errorf("storage: installing compacted wal: %w", err)
	}
	if err := d.wal.Close(); err != nil {
		return fmt.Errorf("storage: closing old wal handle: %w", err)
	}
	newHandle, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("storage: reopening compacted wal: %w", err)
	}
	if _, err := newHandle.Seek(0, io.SeekEnd); err != nil {
		newHandle.Close()
		return fmt.Errorf("storage: seeking new wal handle: %w", err)
	}

	d.wal = newHandle
	d.offsets = newOffsets
	d.lastIncludedIndex = lastIncludedIndex
	d.lastIncludedTerm = lastIncludedTerm
	return nil
}

// Load reconstructs the full persisted state. Any torn or corrupt trailing
// WAL record — the signature of a crash mid-append — is dropped, and the
// file is truncated to the last valid record boundary so future appends
// start from clean ground.
func (d *DiskStorage) Load() (raft.PersistentState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	term, votedFor, err := d.loadHardState()
	if err != nil {
		return raft.PersistentState{}, err
	}

	lastIncludedIndex, lastIncludedTerm, snapData, err := d.loadSnapshot()
	if err != nil {
		return raft.PersistentState{}, err
	}
	d.lastIncludedIndex = lastIncludedIndex
	d.lastIncludedTerm = lastIncludedTerm

	entries, offsets, err := d.scanAndRepairWAL(lastIncludedIndex)
	if err != nil {
		return raft.PersistentState{}, err
	}
	d.offsets = offsets

	log := make([]raft.LogEntry, 0, len(entries)+1)
	log = append(log, raft.LogEntry{Term: lastIncludedTerm, Index: lastIncludedIndex})
	log = append(log, entries...)

	return raft.PersistentState{
		CurrentTerm:  term,
		VotedFor:     votedFor,
		Log:          log,
		SnapshotData: snapData,
	}, nil
}

func (d *DiskStorage) loadSnapshot() (lastIncludedIndex, lastIncludedTerm uint64, data []byte, err error) {
	raw, err := os.ReadFile(filepath.Join(d.dir, snapshotFileName))
	if os.IsNotExist(err) {
		return 0, 0, nil, nil
	}
	if err != nil {
		return 0, 0, nil, fmt.Errorf("storage: reading snapshot: %w", err)
	}
	if len(raw) < 20 {
		return 0, 0, nil, fmt.Errorf("storage: snapshot file too short (%d bytes)", len(raw))
	}
	idx := binary.BigEndian.Uint64(raw[0:8])
	term := binary.BigEndian.Uint64(raw[8:16])
	dataLen := binary.BigEndian.Uint32(raw[16:20])
	if int(20+dataLen) != len(raw) {
		return 0, 0, nil, fmt.Errorf("storage: snapshot data length mismatch")
	}
	var d2 []byte
	if dataLen > 0 {
		d2 = append([]byte(nil), raw[20:]...)
	}
	return idx, term, d2, nil
}

func (d *DiskStorage) loadHardState() (uint64, transport.NodeID, error) {
	data, err := os.ReadFile(filepath.Join(d.dir, hardStateFileName))
	if os.IsNotExist(err) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("storage: reading hardstate: %w", err)
	}
	if len(data) < 12 {
		return 0, "", fmt.Errorf("storage: hardstate file too short (%d bytes)", len(data))
	}
	term := binary.BigEndian.Uint64(data[0:8])
	n := binary.BigEndian.Uint32(data[8:12])
	if int(12+n) != len(data) {
		return 0, "", fmt.Errorf("storage: hardstate votedFor length mismatch")
	}
	return term, transport.NodeID(data[12:]), nil
}

// scanAndRepairWAL reads every valid record from the start of the file,
// stopping at the first torn header, torn payload, checksum mismatch, or
// out-of-sequence index — any of which mean everything from that point on
// was never durably completed — and physically truncates the file there.
// baseIndex is the current snapshot's lastIncludedIndex (0 if none), since
// the first record in the WAL is for baseIndex+1.
func (d *DiskStorage) scanAndRepairWAL(baseIndex uint64) ([]raft.LogEntry, []int64, error) {
	if _, err := d.wal.Seek(0, io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("storage: seeking wal: %w", err)
	}
	r := bufio.NewReader(d.wal)

	var entries []raft.LogEntry
	var offsets []int64
	var pos int64
	expectedIndex := baseIndex + 1

	for {
		header := make([]byte, recordHeaderSize)
		if _, err := io.ReadFull(r, header); err != nil {
			break // clean EOF or a torn header; either way, stop here.
		}
		payloadLen := binary.BigEndian.Uint32(header[0:4])
		wantCRC := binary.BigEndian.Uint32(header[4:8])
		if payloadLen > maxRecordPayload {
			break // implausible length: treat as corruption, not an OOM risk.
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // torn payload: header landed on disk, body didn't.
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			break // corrupt record.
		}
		entry, err := decodeEntry(payload)
		if err != nil || entry.Index != expectedIndex {
			break // corrupt record or unexpected gap.
		}

		entries = append(entries, entry)
		offsets = append(offsets, pos)
		pos += recordHeaderSize + int64(payloadLen)
		expectedIndex++
	}

	if err := d.wal.Truncate(pos); err != nil {
		return nil, nil, fmt.Errorf("storage: truncating torn wal tail: %w", err)
	}
	if _, err := d.wal.Seek(pos, io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("storage: seeking wal after repair: %w", err)
	}
	if err := d.wal.Sync(); err != nil {
		return nil, nil, fmt.Errorf("storage: fsyncing wal after repair: %w", err)
	}

	return entries, offsets, nil
}

func encodeEntry(e raft.LogEntry) []byte {
	buf := make([]byte, 8+8+4+len(e.Command))
	binary.BigEndian.PutUint64(buf[0:8], e.Term)
	binary.BigEndian.PutUint64(buf[8:16], e.Index)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(e.Command)))
	copy(buf[20:], e.Command)
	return buf
}

func decodeEntry(payload []byte) (raft.LogEntry, error) {
	if len(payload) < 20 {
		return raft.LogEntry{}, fmt.Errorf("storage: truncated entry payload")
	}
	term := binary.BigEndian.Uint64(payload[0:8])
	index := binary.BigEndian.Uint64(payload[8:16])
	cmdLen := binary.BigEndian.Uint32(payload[16:20])
	if int(cmdLen) != len(payload)-20 {
		return raft.LogEntry{}, fmt.Errorf("storage: entry command length mismatch")
	}
	var cmd []byte
	if cmdLen > 0 {
		cmd = append([]byte(nil), payload[20:]...)
	}
	return raft.LogEntry{Term: term, Index: index, Command: cmd}, nil
}
