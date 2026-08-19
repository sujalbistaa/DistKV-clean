// Package kv implements the replicated key-value state machine applied
// deterministically from a raft.Node's log.
package kv

// Locking: mu guards data and sessions. Apply is only ever meant to be
// called by a single goroutine, in log order (see Server.runApplyLoop) —
// mu exists so Snapshot/Restore, which run from a different goroutine, can
// safely run concurrently with it, not to allow concurrent Apply calls.

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// Store is the key-value state machine: a plain map plus, for every
// client session seen, the sequence number and reply of its most recent
// mutating request, so a retried Put/Append/Delete is answered from cache
// instead of being applied a second time.
type Store struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[string]session
}

type session struct {
	seqNo uint64
	reply *pb.CommandReply
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{
		data:     make(map[string]string),
		sessions: make(map[string]session),
	}
}

// Apply decodes a marshaled Command (the exact bytes a raft.LogEntry.Command
// holds) and applies it, returning its reply. It must be called exactly
// once per committed log index, in index order; Apply itself trusts that
// invariant rather than re-deriving it.
func (s *Store) Apply(commandBytes []byte) (*pb.Command, *pb.CommandReply) {
	var cmd pb.Command
	if err := proto.Unmarshal(commandBytes, &cmd); err != nil {
		return &cmd, &pb.CommandReply{Success: false, Error: fmt.Sprintf("kv: decoding command: %v", err)}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	dedupes := cmd.Op != pb.Op_OP_GET
	if dedupes {
		if sess, ok := s.sessions[cmd.ClientId]; ok {
			switch {
			case cmd.SeqNo == sess.seqNo:
				return &cmd, sess.reply // retried request: cached reply, not reapplied.
			case cmd.SeqNo < sess.seqNo:
				// Stale retry of an even older request than the one we
				// last recorded for this client; nothing to apply or
				// report, but not an error.
				return &cmd, &pb.CommandReply{Success: true}
			}
		}
	}

	reply := s.applyLocked(&cmd)

	if dedupes {
		s.sessions[cmd.ClientId] = session{seqNo: cmd.SeqNo, reply: reply}
	}
	return &cmd, reply
}

func (s *Store) applyLocked(cmd *pb.Command) *pb.CommandReply {
	switch cmd.Op {
	case pb.Op_OP_GET:
		v, ok := s.data[cmd.Key]
		return &pb.CommandReply{Success: true, Value: v, Found: ok}
	case pb.Op_OP_PUT:
		s.data[cmd.Key] = cmd.Value
		return &pb.CommandReply{Success: true}
	case pb.Op_OP_APPEND:
		s.data[cmd.Key] += cmd.Value
		return &pb.CommandReply{Success: true}
	case pb.Op_OP_DELETE:
		delete(s.data, cmd.Key)
		return &pb.CommandReply{Success: true}
	default:
		return &pb.CommandReply{Success: false, Error: fmt.Sprintf("kv: unknown op %v", cmd.Op)}
	}
}

// Len returns how many keys the store currently holds. It is derived
// state for monitoring, not part of the state machine.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Snapshot marshals the store's entire current state (key-value data and
// every session's dedup record) into the opaque blob raft.Node.Snapshot
// expects.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := &pb.Snapshot{
		Data:     make(map[string]string, len(s.data)),
		Sessions: make(map[string]*pb.SessionState, len(s.sessions)),
	}
	for k, v := range s.data {
		snap.Data[k] = v
	}
	for id, sess := range s.sessions {
		snap.Sessions[id] = &pb.SessionState{SeqNo: sess.seqNo, Reply: sess.reply}
	}
	return proto.Marshal(snap)
}

// Restore replaces the store's entire state with the one encoded in data,
// as produced by Snapshot (locally or by another node, and shipped here
// via InstallSnapshot). It's the counterpart to a SnapshotValid ApplyMsg.
func (s *Store) Restore(data []byte) error {
	var snap pb.Snapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("kv: decoding snapshot: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]string, len(snap.Data))
	for k, v := range snap.Data {
		s.data[k] = v
	}
	s.sessions = make(map[string]session, len(snap.Sessions))
	for id, ss := range snap.Sessions {
		s.sessions[id] = session{seqNo: ss.SeqNo, reply: ss.Reply}
	}
	return nil
}
