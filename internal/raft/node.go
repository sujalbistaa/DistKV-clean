// Package raft implements the Raft consensus protocol (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", extended version)
// from scratch, against the transport.Transport abstraction so tests can
// run it over a fault-injecting fake network.
package raft

// Locking: mu guards every field of Node from currentTerm down to
// failErr below. id, peers, trans, and the timing configuration are set
// once in NewNode and never mutated afterward, so they need no lock. No
// code path holds mu while sending an RPC or otherwise blocking, so a
// slow or partitioned peer can never stall elections or heartbeats to the
// others.

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/sujalbistaa/DistKV/internal/transport"
)

const (
	defaultElectionTimeoutMin = 150 * time.Millisecond
	defaultElectionTimeoutMax = 300 * time.Millisecond
	defaultHeartbeatInterval  = 50 * time.Millisecond
	defaultTickInterval       = 10 * time.Millisecond
	defaultRPCTimeout         = 75 * time.Millisecond
)

// Config configures a Node. ID, Peers, and Transport are required;
// everything else has a production-sane default and exists so tests can
// shrink timeouts to keep test runtime reasonable.
type Config struct {
	ID        transport.NodeID
	Peers     []transport.NodeID
	Transport transport.Transport
	Storage   Storage

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	TickInterval       time.Duration
	RPCTimeout         time.Duration

	// SnapshotThreshold is how many applied-but-not-yet-snapshotted log
	// entries accumulate before the node signals on SnapshotTrigger() that
	// the application should snapshot. 0 disables automatic triggering;
	// Snapshot can still be called directly at any time.
	SnapshotThreshold int

	Seed   int64
	Logger *log.Logger
}

// Node is one member of a Raft cluster.
type Node struct {
	id    transport.NodeID
	peers []transport.NodeID
	trans transport.Transport

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
	tickInterval       time.Duration
	rpcTimeout         time.Duration
	logger             *log.Logger

	mu               sync.Mutex
	storage          Storage
	rng              *rand.Rand
	role             Role
	currentTerm      uint64
	votedFor         transport.NodeID
	leaderID         transport.NodeID
	log              []LogEntry // slice position 0 is the boundary entry; see LogEntry
	commitIndex      uint64
	nextIndex        map[transport.NodeID]uint64
	matchIndex       map[transport.NodeID]uint64
	electionDeadline time.Time
	nextHeartbeat    time.Time
	failed           bool
	failErr          error

	// lastIncludedIndex/lastIncludedTerm/snapshotData mirror log[0]: the
	// most recent snapshot's boundary and the application's opaque state
	// as of that point. lastIncludedIndex is 0 if no snapshot exists yet.
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	snapshotData      []byte

	lastApplied uint64
	// pendingSnapshot, when non-nil, is delivered on applyCh before any
	// further commands, ahead of the normal lastApplied<commitIndex loop:
	// either a snapshot loaded at startup or one just installed by the
	// leader.
	pendingSnapshot *ApplyMsg
	applyCh         chan ApplyMsg
	applyWake       chan struct{}

	snapshotThreshold       int
	snapshotTriggerCh       chan uint64
	lastSnapshotRequestedAt uint64

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewNode constructs a Node from cfg, loads any persisted state, registers
// itself as cfg.Transport's handler, and starts its background
// election/heartbeat loop. The returned Node is immediately live.
func NewNode(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("raft: Config.ID is required")
	}
	if cfg.Transport == nil {
		return nil, fmt.Errorf("raft: Config.Transport is required")
	}

	storage := cfg.Storage
	if storage == nil {
		storage = NewMemoryStorage()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}

	n := &Node{
		id:                 cfg.ID,
		peers:              append([]transport.NodeID(nil), cfg.Peers...),
		trans:              cfg.Transport,
		electionTimeoutMin: orDefault(cfg.ElectionTimeoutMin, defaultElectionTimeoutMin),
		electionTimeoutMax: orDefault(cfg.ElectionTimeoutMax, defaultElectionTimeoutMax),
		heartbeatInterval:  orDefault(cfg.HeartbeatInterval, defaultHeartbeatInterval),
		tickInterval:       orDefault(cfg.TickInterval, defaultTickInterval),
		rpcTimeout:         orDefault(cfg.RPCTimeout, defaultRPCTimeout),
		logger:             logger,
		storage:            storage,
		rng:                rand.New(rand.NewSource(cfg.Seed)),
		role:               Follower,
		applyCh:            make(chan ApplyMsg, 64),
		applyWake:          make(chan struct{}, 1),
		snapshotThreshold:  cfg.SnapshotThreshold,
		snapshotTriggerCh:  make(chan uint64, 1),
		stopCh:             make(chan struct{}),
	}
	if n.electionTimeoutMax < n.electionTimeoutMin {
		return nil, fmt.Errorf("raft: ElectionTimeoutMax must be >= ElectionTimeoutMin")
	}

	state, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: loading persisted state: %w", err)
	}
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	n.log = state.Log
	n.lastIncludedIndex = state.Log[0].Index
	n.lastIncludedTerm = state.Log[0].Term
	n.snapshotData = state.SnapshotData
	n.commitIndex = n.lastIncludedIndex
	n.lastApplied = n.lastIncludedIndex
	n.lastSnapshotRequestedAt = n.lastIncludedIndex
	if n.lastIncludedIndex > 0 {
		n.pendingSnapshot = &ApplyMsg{
			SnapshotValid: true,
			Snapshot:      n.snapshotData,
			SnapshotIndex: n.lastIncludedIndex,
			SnapshotTerm:  n.lastIncludedTerm,
		}
	}

	n.mu.Lock()
	n.resetElectionDeadlineLocked()
	n.mu.Unlock()

	cfg.Transport.RegisterHandler(n.handle)

	n.wg.Add(1)
	go n.run()
	n.wg.Add(1)
	go n.applyLoop()

	return n, nil
}

func orDefault(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}

// Stop halts the node's background loop and waits for it to exit. It does
// not close the underlying transport.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
	n.wg.Wait()
}

// ID returns this node's identity.
func (n *Node) ID() transport.NodeID { return n.id }

// State returns the current term and role.
func (n *Node) State() (term uint64, role Role) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role
}

// Leader returns the NodeID this node currently believes is the leader, or
// "" if it doesn't know of one.
func (n *Node) Leader() transport.NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Status is a point-in-time snapshot of a Node's state, for monitoring and
// tests. It is not part of the replicated state machine.
type Status struct {
	ID          transport.NodeID
	Term        uint64
	Role        Role
	LeaderID    transport.NodeID
	CommitIndex uint64
	LastApplied uint64

	// LastIncludedIndex/LastIncludedTerm are the boundary of the most
	// recent snapshot (0 if none). LogLen is how many entries this node
	// currently holds in memory beyond that boundary — the quantity a
	// snapshot threshold bounds.
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	LogLen            int

	// LastLogIndex is the index of the last entry this node holds,
	// snapshot boundary included. On a leader it is the high-water mark
	// followers are being driven towards.
	LastLogIndex uint64

	// Followers is the leader's view of how far each peer has got: the
	// nextIndex/matchIndex pair it maintains for them. It is nil on a
	// node that isn't leader, which keeps "I have no followers" and "I am
	// not the one tracking followers" distinguishable.
	Followers map[transport.NodeID]FollowerProgress
}

// FollowerProgress is a leader's replication bookkeeping for one peer.
type FollowerProgress struct {
	NextIndex  uint64
	MatchIndex uint64
}

// Status returns a snapshot of this node's current state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()

	var followers map[transport.NodeID]FollowerProgress
	if n.role == Leader {
		followers = make(map[transport.NodeID]FollowerProgress, len(n.peers))
		for _, peer := range n.peers {
			followers[peer] = FollowerProgress{
				NextIndex:  n.nextIndex[peer],
				MatchIndex: n.matchIndex[peer],
			}
		}
	}

	return Status{
		ID:                n.id,
		Term:              n.currentTerm,
		Role:              n.role,
		LeaderID:          n.leaderID,
		CommitIndex:       n.commitIndex,
		LastApplied:       n.lastApplied,
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
		LogLen:            len(n.log) - 1,
		LastLogIndex:      n.lastIndexLocked(),
		Followers:         followers,
	}
}

// ApplyCh returns the channel on which committed log entries and installed
// snapshots are delivered, in strictly increasing index order with no gaps
// or duplicates. It is closed once the node stops.
func (n *Node) ApplyCh() <-chan ApplyMsg {
	return n.applyCh
}

// SnapshotTrigger signals the index the application should snapshot up to,
// whenever Config.SnapshotThreshold worth of entries have accumulated
// since the last snapshot. The channel is coalescing (buffered 1): a slow
// consumer sees the latest eligible index, not every threshold crossing.
func (n *Node) SnapshotTrigger() <-chan uint64 {
	return n.snapshotTriggerCh
}

// Snapshot tells the node that the application has durably captured its
// entire state as of index — which must already have been applied — in
// data, so every log entry at or before index can be discarded. It is a
// no-op if index is not beyond whatever this node has already snapshotted,
// so a caller reacting to SnapshotTrigger need not worry about racing a
// more recent snapshot.
func (n *Node) Snapshot(index uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.failed {
		return n.failErr
	}
	if index <= n.lastIncludedIndex {
		return nil
	}
	if index > n.lastApplied {
		return fmt.Errorf("raft: Snapshot: index %d has not been applied yet (lastApplied=%d)", index, n.lastApplied)
	}

	term := n.entryAtLocked(index).Term
	newLog := append([]LogEntry(nil), n.log[index-n.lastIncludedIndex:]...)
	newLog[0].Command = nil

	if err := n.storage.SaveSnapshot(index, term, data); err != nil {
		n.disableLocked(fmt.Errorf("saving snapshot: %w", err))
		return n.failErr
	}
	n.log = newLog
	n.lastIncludedIndex = index
	n.lastIncludedTerm = term
	n.snapshotData = data
	return nil
}

// Propose appends command to the leader's log and kicks off replication
// immediately rather than waiting for the next heartbeat. It returns the
// index and term the entry was given and whether this node currently
// believes itself to be leader; if not, the command is not appended and
// isLeader is false.
//
// Appearing at (index, term) on ApplyCh is the only proof the command
// committed. If a different command later appears at the same index, this
// leader was deposed before the entry committed, the original proposal was
// lost, and the caller should retry.
func (n *Node) Propose(command []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	if n.failed || n.role != Leader {
		n.mu.Unlock()
		return 0, 0, false
	}
	index = n.lastIndexLocked() + 1
	term = n.currentTerm
	n.appendLogLocked([]LogEntry{{
		Term:    term,
		Index:   index,
		Command: append([]byte(nil), command...),
	}})
	if n.failed {
		n.mu.Unlock()
		return 0, 0, false
	}
	n.maybeAdvanceCommitLocked()
	n.mu.Unlock()

	n.replicateToPeers()
	return index, term, true
}

// handle dispatches an incoming RPC to the appropriate typed handler. It is
// registered with the transport as this node's single entry point for
// inbound traffic.
func (n *Node) handle(ctx context.Context, from transport.NodeID, req any) (any, error) {
	switch args := req.(type) {
	case *RequestVoteArgs:
		return n.handleRequestVote(args)
	case *AppendEntriesArgs:
		return n.handleAppendEntries(args)
	case *InstallSnapshotArgs:
		return n.handleInstallSnapshot(args)
	default:
		return nil, fmt.Errorf("raft: %s: unknown request type %T", n.id, req)
	}
}

func (n *Node) handleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.failed {
		return nil, n.failErr
	}

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
		if n.failed {
			return nil, n.failErr
		}
	}

	reply := &RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}

	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	upToDate := n.candidateLogUpToDateLocked(args.LastLogIndex, args.LastLogTerm)
	if canVote && upToDate {
		n.saveHardStateLocked(n.currentTerm, args.CandidateID)
		if n.failed {
			return nil, n.failErr
		}
		n.resetElectionDeadlineLocked()
		reply.VoteGranted = true
	}
	return reply, nil
}

func (n *Node) handleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.failed {
		return nil, n.failErr
	}

	if args.Term < n.currentTerm {
		return &AppendEntriesReply{Term: n.currentTerm, Success: false}, nil
	}

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	} else if n.role == Candidate {
		// A candidate that hears from a legitimate leader for its own
		// term recognizes it and steps down (Raft paper, section 5.2).
		n.role = Follower
	}
	if n.failed {
		return nil, n.failErr
	}

	// A valid AppendEntries from the current-term leader proves the leader
	// is alive and resets our election timer, whether or not the
	// consistency check below accepts its entries.
	n.leaderID = args.LeaderID
	n.resetElectionDeadlineLocked()

	if args.PrevLogIndex < n.lastIncludedIndex {
		// We've already snapshotted past this point, so everything up to
		// our snapshot boundary is implicitly consistent (it's
		// committed). Drop the portion of Entries at or before our
		// boundary and re-anchor PrevLogIndex/PrevLogTerm there.
		skip := n.lastIncludedIndex - args.PrevLogIndex
		if uint64(len(args.Entries)) <= skip {
			return &AppendEntriesReply{Term: n.currentTerm, Success: true}, nil
		}
		args = &AppendEntriesArgs{
			Term:         args.Term,
			LeaderID:     args.LeaderID,
			PrevLogIndex: n.lastIncludedIndex,
			PrevLogTerm:  n.lastIncludedTerm,
			Entries:      args.Entries[skip:],
			LeaderCommit: args.LeaderCommit,
		}
	}

	// Consistency check (Raft paper, AppendEntries receiver rule 2), with
	// the extended paper's fast-backup optimisation: instead of the leader
	// retrying one index at a time on failure, the reply carries enough
	// information for it to jump straight to the right nextIndex.
	if args.PrevLogIndex > n.lastIndexLocked() {
		return &AppendEntriesReply{
			Term:          n.currentTerm,
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: n.lastIndexLocked() + 1,
		}, nil
	}
	if prevTerm := n.termAtLocked(args.PrevLogIndex); prevTerm != args.PrevLogTerm {
		return &AppendEntriesReply{
			Term:          n.currentTerm,
			Success:       false,
			ConflictTerm:  prevTerm,
			ConflictIndex: n.firstIndexOfTermLocked(prevTerm),
		}, nil
	}

	// Receiver rules 3/4: find the first index where our log disagrees
	// with the incoming entries, truncate there, and append the rest. If
	// every incoming entry already matches what we have (a duplicated or
	// retried RPC), this is a no-op.
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= n.lastIndexLocked() && n.termAtLocked(idx) == e.Term {
			continue
		}
		if idx <= n.lastIndexLocked() {
			n.truncateLogLocked(idx)
			if n.failed {
				return nil, n.failErr
			}
		}
		n.appendLogLocked(args.Entries[i:])
		if n.failed {
			return nil, n.failErr
		}
		break
	}

	// Receiver rule 5.
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastIndexLocked())
		n.notifyApply()
	}

	return &AppendEntriesReply{Term: n.currentTerm, Success: true}, nil
}

// handleInstallSnapshot accepts a leader's snapshot (Raft paper, section 7):
// persists it, replaces or discards our log up to its boundary, and queues
// it for delivery to the application on ApplyCh ahead of any further
// commands.
func (n *Node) handleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.failed {
		return nil, n.failErr
	}
	if args.Term < n.currentTerm {
		return &InstallSnapshotReply{Term: n.currentTerm}, nil
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	} else if n.role == Candidate {
		n.role = Follower
	}
	if n.failed {
		return nil, n.failErr
	}
	n.leaderID = args.LeaderID
	n.resetElectionDeadlineLocked()

	if args.LastIncludedIndex <= n.lastIncludedIndex {
		// Stale or duplicate: we're already at least this current.
		return &InstallSnapshotReply{Term: n.currentTerm}, nil
	}

	if err := n.storage.SaveSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data); err != nil {
		n.disableLocked(fmt.Errorf("saving installed snapshot: %w", err))
		return nil, n.failErr
	}

	if args.LastIncludedIndex <= n.lastIndexLocked() && n.termAtLocked(args.LastIncludedIndex) == args.LastIncludedTerm {
		// Our log already reaches the snapshot point and agrees with it:
		// keep our tail beyond it, just drop the now-redundant prefix.
		n.log = append([]LogEntry(nil), n.log[args.LastIncludedIndex-n.lastIncludedIndex:]...)
	} else {
		// Our log conflicts with, or doesn't reach, the snapshot: discard
		// it entirely and start fresh from the snapshot boundary.
		n.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}
	n.lastIncludedIndex = args.LastIncludedIndex
	n.lastIncludedTerm = args.LastIncludedTerm
	n.snapshotData = args.Data
	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	if n.lastApplied < args.LastIncludedIndex {
		n.lastApplied = args.LastIncludedIndex
	}
	n.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	n.notifyApply()

	return &InstallSnapshotReply{Term: n.currentTerm}, nil
}

func (n *Node) candidateLogUpToDateLocked(lastLogIndex, lastLogTerm uint64) bool {
	myIndex, myTerm := n.lastLogInfoLocked()
	if lastLogTerm != myTerm {
		return lastLogTerm > myTerm
	}
	return lastLogIndex >= myIndex
}

func (n *Node) lastLogInfoLocked() (index, term uint64) {
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// becomeFollowerLocked converts to Follower. If term is newer than
// currentTerm, currentTerm advances and any vote already cast this term is
// forgotten, per the Raft paper's rule that a stale term is never sticky.
func (n *Node) becomeFollowerLocked(term uint64) {
	n.role = Follower
	if term > n.currentTerm {
		n.saveHardStateLocked(term, "")
	}
}

func (n *Node) becomeCandidateLocked() {
	n.role = Candidate
	n.leaderID = ""
	n.saveHardStateLocked(n.currentTerm+1, n.id)
	if n.failed {
		return
	}
	n.resetElectionDeadlineLocked()
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id
	last := n.lastIndexLocked()
	n.nextIndex = make(map[transport.NodeID]uint64, len(n.peers))
	n.matchIndex = make(map[transport.NodeID]uint64, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	// Send heartbeats immediately rather than waiting a full interval, so
	// followers and any deposed leader learn about the new term quickly.
	n.nextHeartbeat = time.Time{}
	n.logger.Printf("raft: %s became leader for term %d", n.id, n.currentTerm)
}

func (n *Node) lastIndexLocked() uint64 {
	return n.log[len(n.log)-1].Index
}

// entryAtLocked returns the log entry at absolute index, which must satisfy
// lastIncludedIndex <= index <= lastIndexLocked(): log entries at slice
// position 0 (the snapshot boundary) upward, translated from the absolute
// index space to the slice's, since a snapshot may have trimmed away
// everything before lastIncludedIndex.
func (n *Node) entryAtLocked(index uint64) LogEntry {
	return n.log[index-n.lastIncludedIndex]
}

// termAtLocked returns the term of the entry at index, which must be in
// [lastIncludedIndex, lastIndexLocked()].
func (n *Node) termAtLocked(index uint64) uint64 {
	return n.entryAtLocked(index).Term
}

// firstIndexOfTermLocked returns the lowest index in our log holding term,
// used to tell a leader where a conflicting term begins when the leader has
// no entries of that term itself. It considers the boundary entry at slice
// position 0 too, since after a snapshot that may itself be the answer.
func (n *Node) firstIndexOfTermLocked(term uint64) uint64 {
	if term == 0 {
		return 1
	}
	for pos := range n.log {
		if n.log[pos].Term == term {
			return n.log[pos].Index
		}
	}
	return n.lastIncludedIndex + 1
}

// lastIndexOfTermLocked returns the highest index in our log holding term,
// if any, used by a leader that does have entries of a follower's
// conflicting term to jump nextIndex just past them.
func (n *Node) lastIndexOfTermLocked(term uint64) (uint64, bool) {
	for pos := len(n.log) - 1; pos >= 0; pos-- {
		if n.log[pos].Term == term {
			return n.log[pos].Index, true
		}
		if n.log[pos].Term < term {
			break
		}
	}
	return 0, false
}

// maybeAdvanceCommitLocked implements the paper's section 5.4.2 rule: a
// leader may only conclude an entry is committed by counting replicas of an
// entry from its own current term. Older-term entries commit only
// indirectly, as a side effect of a later current-term entry committing,
// since a majority replicating that later entry implies they hold every
// entry before it too.
func (n *Node) maybeAdvanceCommitLocked() {
	if n.role != Leader {
		return
	}
	majority := len(n.peers)/2 + 1
	for idx := n.lastIndexLocked(); idx > n.commitIndex; idx-- {
		if n.termAtLocked(idx) != n.currentTerm {
			continue
		}
		count := 1 // the leader itself
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = idx
			n.notifyApply()
			return
		}
	}
}

// notifyApply wakes applyLoop if it's waiting; safe to call with or without
// mu held, and safe to call more than once before applyLoop wakes; the
// buffered wake channel coalesces redundant notifications.
func (n *Node) notifyApply() {
	select {
	case n.applyWake <- struct{}{}:
	default:
	}
}

// saveHardStateLocked updates currentTerm/votedFor in memory and persists
// them together, since a torn write that recorded one but not the other
// could double-vote after a crash.
func (n *Node) saveHardStateLocked(term uint64, votedFor transport.NodeID) {
	n.currentTerm = term
	n.votedFor = votedFor
	if err := n.storage.SaveHardState(term, votedFor); err != nil {
		n.disableLocked(fmt.Errorf("persisting hard state: %w", err))
	}
}

// appendLogLocked appends entries to the in-memory log and persists them.
func (n *Node) appendLogLocked(entries []LogEntry) {
	n.log = append(n.log, entries...)
	if err := n.storage.AppendLog(entries); err != nil {
		n.disableLocked(fmt.Errorf("appending log: %w", err))
	}
}

// truncateLogLocked discards every in-memory and persisted entry at index
// >= fromIndex, which must be strictly greater than lastIncludedIndex.
func (n *Node) truncateLogLocked(fromIndex uint64) {
	n.log = n.log[:fromIndex-n.lastIncludedIndex]
	if err := n.storage.TruncateLog(fromIndex); err != nil {
		n.disableLocked(fmt.Errorf("truncating log: %w", err))
	}
}

// disableLocked marks the node permanently unable to participate after a
// persist failure. Granting a vote or claiming a term we might forget on
// crash would risk double-voting or split-brain, so the node stops taking
// part rather than continuing on state it can no longer trust is durable.
func (n *Node) disableLocked(err error) {
	n.failed = true
	n.failErr = fmt.Errorf("raft: %s: %w, node disabled", n.id, err)
	n.logger.Printf("%v", n.failErr)
}

func (n *Node) resetElectionDeadlineLocked() {
	span := int64(n.electionTimeoutMax - n.electionTimeoutMin)
	var extra time.Duration
	if span > 0 {
		extra = time.Duration(n.rng.Int63n(span))
	}
	n.electionDeadline = time.Now().Add(n.electionTimeoutMin + extra)
}

func (n *Node) run() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	if n.failed {
		n.mu.Unlock()
		return
	}
	now := time.Now()
	startElection := n.role != Leader && now.After(n.electionDeadline)
	sendHeartbeats := n.role == Leader && now.After(n.nextHeartbeat)
	if sendHeartbeats {
		n.nextHeartbeat = now.Add(n.heartbeatInterval)
	}
	var snapshotRequestIndex uint64
	if n.snapshotThreshold > 0 &&
		n.lastApplied-n.lastIncludedIndex >= uint64(n.snapshotThreshold) &&
		n.lastApplied > n.lastSnapshotRequestedAt {
		n.lastSnapshotRequestedAt = n.lastApplied
		snapshotRequestIndex = n.lastApplied
	}
	n.mu.Unlock()

	if startElection {
		n.startElection()
	}
	if sendHeartbeats {
		n.replicateToPeers()
	}
	if snapshotRequestIndex > 0 {
		select {
		case n.snapshotTriggerCh <- snapshotRequestIndex:
		default:
		}
	}
}

// startElection converts to Candidate, votes for itself, and requests votes
// from every peer in parallel. Replies are processed as they arrive; a
// stale reply (from a term or role this node has since moved past) is
// discarded.
func (n *Node) startElection() {
	n.mu.Lock()
	if n.failed {
		n.mu.Unlock()
		return
	}
	n.becomeCandidateLocked()
	if n.failed {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	id := n.id
	lastIndex, lastTerm := n.lastLogInfoLocked()
	peers := append([]transport.NodeID(nil), n.peers...)
	n.mu.Unlock()

	n.logger.Printf("raft: %s starting election for term %d", id, term)

	majority := len(peers)/2 + 1
	votes := 1 // vote for self

	if votes >= majority {
		// Single-node cluster: no peers to wait on.
		n.mu.Lock()
		if n.role == Candidate && n.currentTerm == term {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		return
	}

	args := &RequestVoteArgs{Term: term, CandidateID: id, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
	for _, peer := range peers {
		peer := peer
		go func() {
			reply := n.sendRequestVote(peer, args)
			if reply == nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.failed {
				return
			}
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			if n.role != Candidate || n.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes >= majority {
				n.becomeLeaderLocked()
			}
		}()
	}
}

func (n *Node) sendRequestVote(peer transport.NodeID, args *RequestVoteArgs) *RequestVoteReply {
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()
	resp, err := n.trans.Send(ctx, peer, args)
	if err != nil {
		return nil
	}
	reply, ok := resp.(*RequestVoteReply)
	if !ok {
		return nil
	}
	return reply
}

// replicateToPeers sends each peer an AppendEntries carrying whatever
// entries it needs next (possibly none, in which case this is a plain
// heartbeat), in parallel. It is called both periodically, as the leader's
// heartbeat, and immediately after Propose, to keep commit latency low.
func (n *Node) replicateToPeers() {
	n.mu.Lock()
	if n.failed || n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	id := n.id
	peers := append([]transport.NodeID(nil), n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(peer, term, id)
	}
}

// replicateToPeer sends peer an AppendEntries built from this leader's
// current view of peer's nextIndex, and applies the reply: on success,
// advances matchIndex/nextIndex and tries to advance commitIndex; on
// failure, jumps nextIndex back using the fast-backup fields in the reply.
func (n *Node) replicateToPeer(peer transport.NodeID, term uint64, id transport.NodeID) {
	n.mu.Lock()
	if n.failed || n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	if nextIdx <= n.lastIncludedIndex {
		// The leader no longer has what this follower needs next: it was
		// compacted away by a snapshot. Only a full snapshot can catch
		// this follower up.
		n.mu.Unlock()
		n.sendInstallSnapshot(peer, term, id)
		return
	}
	prevIndex := nextIdx - 1
	prevTerm := n.termAtLocked(prevIndex)
	entries := append([]LogEntry(nil), n.log[nextIdx-n.lastIncludedIndex:]...)
	commit := n.commitIndex
	n.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: commit,
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()
	resp, err := n.trans.Send(ctx, peer, args)
	if err != nil {
		return
	}
	reply, ok := resp.(*AppendEntriesReply)
	if !ok {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failed {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return // stale: no longer leader, or leader of a later term
	}

	if reply.Success {
		newMatch := prevIndex + uint64(len(entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		if newMatch+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = newMatch + 1
		}
		n.maybeAdvanceCommitLocked()
		return
	}

	if reply.ConflictTerm == 0 {
		n.nextIndex[peer] = max(uint64(1), reply.ConflictIndex)
		return
	}
	if idx, ok := n.lastIndexOfTermLocked(reply.ConflictTerm); ok {
		n.nextIndex[peer] = idx + 1
	} else {
		n.nextIndex[peer] = max(uint64(1), reply.ConflictIndex)
	}
}

// sendInstallSnapshot sends peer this leader's current snapshot in full (a
// simplification of the paper's chunked offset/done RPC scheme; see
// docs/design.md) and, on success, advances matchIndex/nextIndex past it
// exactly as a successful AppendEntries would.
func (n *Node) sendInstallSnapshot(peer transport.NodeID, term uint64, id transport.NodeID) {
	n.mu.Lock()
	if n.failed || n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          id,
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
		Data:              n.snapshotData,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()
	resp, err := n.trans.Send(ctx, peer, args)
	if err != nil {
		return
	}
	reply, ok := resp.(*InstallSnapshotReply)
	if !ok {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failed {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return
	}
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > n.nextIndex[peer] {
		n.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	n.maybeAdvanceCommitLocked()
}

// applyLoop delivers committed entries and installed snapshots on applyCh
// in order, waking whenever notifyApply signals new work. It is the sole
// writer to applyCh and closes it on the way out, once stopCh closes.
func (n *Node) applyLoop() {
	defer n.wg.Done()
	defer close(n.applyCh)
	for {
		n.mu.Lock()
		if n.pendingSnapshot != nil {
			msg := *n.pendingSnapshot
			n.pendingSnapshot = nil
			n.mu.Unlock()
			select {
			case n.applyCh <- msg:
			case <-n.stopCh:
				return
			}
			continue
		}
		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			entry := n.entryAtLocked(n.lastApplied)
			n.mu.Unlock()
			select {
			case n.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}:
			case <-n.stopCh:
				return
			}
			n.mu.Lock()
			if n.pendingSnapshot != nil {
				// A snapshot arrived mid-catch-up; deliver it next, ahead
				// of whatever of this batch remains.
				break
			}
		}
		n.mu.Unlock()

		select {
		case <-n.applyWake:
		case <-n.stopCh:
			return
		}
	}
}
