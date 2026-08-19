package raft

// Locking: mu guards every field below.

import (
	"fmt"
	"sync"

	"github.com/sujalbistaa/DistKV/internal/transport"
)

// MemoryStorage is a Storage implementation that keeps state in memory
// only; a crash loses everything. It's the default Storage for tests that
// don't specifically exercise crash recovery; storage.DiskStorage is the
// real, disk-backed implementation used in production.
type MemoryStorage struct {
	mu          sync.Mutex
	currentTerm uint64
	votedFor    transport.NodeID
	// log holds entries strictly after lastIncludedIndex: log[i] has
	// absolute Index lastIncludedIndex+i+1.
	log               []LogEntry
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	snapshotData      []byte
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

func (m *MemoryStorage) SaveHardState(term uint64, votedFor transport.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTerm = term
	m.votedFor = votedFor
	return nil
}

func (m *MemoryStorage) AppendLog(entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append(m.log, append([]LogEntry(nil), entries...)...)
	return nil
}

func (m *MemoryStorage) TruncateLog(fromIndex uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fromIndex <= m.lastIncludedIndex {
		return fmt.Errorf("raft: TruncateLog: fromIndex %d is at or before the snapshot boundary %d", fromIndex, m.lastIncludedIndex)
	}
	pos := fromIndex - m.lastIncludedIndex - 1
	if int(pos) < len(m.log) {
		m.log = m.log[:pos]
	}
	return nil
}

func (m *MemoryStorage) SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lastIncludedIndex > m.lastIncludedIndex && int(lastIncludedIndex-m.lastIncludedIndex) <= len(m.log) {
		pos := lastIncludedIndex - m.lastIncludedIndex
		m.log = append([]LogEntry(nil), m.log[pos:]...)
	} else {
		m.log = nil
	}
	m.lastIncludedIndex = lastIncludedIndex
	m.lastIncludedTerm = lastIncludedTerm
	m.snapshotData = append([]byte(nil), data...)
	return nil
}

func (m *MemoryStorage) Load() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	log := make([]LogEntry, 0, len(m.log)+1)
	log = append(log, LogEntry{Term: m.lastIncludedTerm, Index: m.lastIncludedIndex})
	log = append(log, m.log...)
	var snap []byte
	if m.snapshotData != nil {
		snap = append([]byte(nil), m.snapshotData...)
	}
	return PersistentState{
		CurrentTerm:  m.currentTerm,
		VotedFor:     m.votedFor,
		Log:          log,
		SnapshotData: snap,
	}, nil
}
