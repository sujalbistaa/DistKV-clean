package raft

import "github.com/sujalbistaa/DistKV/internal/transport"

// Role is a Raft node's current position in the leader-election state
// machine.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is one entry of the replicated log. Slice position 0 of a
// Node's in-memory log is always a boundary entry {lastIncludedTerm,
// lastIncludedIndex} — {0, 0} if no snapshot has ever been taken — so
// PrevLogIndex/PrevLogTerm arithmetic never needs a special case for an
// empty log or a freshly snapshotted one.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// PersistentState is the subset of Raft state that must survive a crash:
// currentTerm, votedFor, the log, and the most recent snapshot, if any
// (section 5.4.1 and section 7 of the Raft paper).
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    transport.NodeID // "" means no vote cast this term
	Log         []LogEntry
	// SnapshotData is the most recently persisted snapshot's opaque
	// application data, or nil if no snapshot has ever been taken.
	// Log[0]'s Term/Index give its lastIncludedTerm/lastIncludedIndex.
	SnapshotData []byte
}

// Storage persists the state that must survive a crash: currentTerm,
// votedFor, the log, and the current snapshot. Every method must not
// return until its effect is durable — Raft relies on that guarantee
// before granting a vote, acknowledging a log entry, or installing a
// snapshot. The methods are deliberately granular (rather than "save the
// whole state") so a real implementation can be a true append-only
// write-ahead log instead of rewriting everything on every call.
// MemoryStorage (this package) is the in-memory default used by tests;
// storage.DiskStorage is the disk-backed production implementation.
type Storage interface {
	// SaveHardState durably persists currentTerm and votedFor.
	SaveHardState(term uint64, votedFor transport.NodeID) error
	// AppendLog durably appends entries to the end of the log. entries[0].Index
	// must be exactly one past whatever is currently persisted.
	AppendLog(entries []LogEntry) error
	// TruncateLog durably discards every persisted entry at index >= fromIndex.
	// fromIndex must be strictly greater than the current snapshot's
	// lastIncludedIndex.
	TruncateLog(fromIndex uint64) error
	// SaveSnapshot durably persists data as the application's state as of
	// lastIncludedIndex/lastIncludedTerm, and discards every persisted log
	// entry at or before lastIncludedIndex, since the snapshot now covers
	// them.
	SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error
	// Load reconstructs the full persisted state, repairing any torn
	// trailing write left by a crash mid-append. The returned Log always
	// includes the boundary entry at slice position 0, even for a brand
	// new store (where it is {0, 0}).
	Load() (PersistentState, error)
}

// RequestVoteArgs is the RequestVote RPC (Raft paper, section 5.2).
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  transport.NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the RequestVote RPC's response.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the AppendEntries RPC (Raft paper, section 5.3).
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     transport.NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the AppendEntries RPC's response.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// ConflictTerm/ConflictIndex implement the fast-backup optimisation
	// from the extended Raft paper: on a failed consistency check, the
	// leader can jump nextIndex straight to the right place instead of
	// retrying one index at a time. ConflictTerm == 0 means the follower's
	// log was simply too short, and ConflictIndex is where to append.
	// Otherwise ConflictTerm is the term of the follower's conflicting
	// entry at PrevLogIndex, and ConflictIndex is the first index in the
	// follower's log that holds that term.
	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshotArgs is the InstallSnapshot RPC (Raft paper, section 7),
// sent when a leader's nextIndex for a follower has already been
// compacted out of the leader's own log. For simplicity this project sends
// a snapshot as a single RPC rather than the paper's chunked offset/done
// scheme; see docs/design.md for the trade-off.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          transport.NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotReply is the InstallSnapshot RPC's response.
type InstallSnapshotReply struct {
	Term uint64
}

// ApplyMsg is delivered on a Node's apply channel in strictly increasing
// index order with no gaps or duplicates. Exactly one of CommandValid or
// SnapshotValid is true: a committed log entry, or a snapshot the
// application must load in place of replaying individual commands (either
// because this node just started up with one persisted, or because it
// just installed one sent by the leader). On SnapshotValid, the
// application must discard whatever state it had and adopt Snapshot as
// its entire state as of SnapshotIndex/SnapshotTerm.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}
