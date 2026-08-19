package kv

// Locking: mu guards waiters only. It is taken briefly to register or
// remove a waiter channel; the channel send/receive itself happens outside
// the lock so a slow client can never block the apply loop.

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/transport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// applyResult pairs a decoded, applied command with its reply, so a
// waiting caller can tell whether the entry that landed at the index it
// was given is really its own request — see execute's comment.
type applyResult struct {
	cmd   *pb.Command
	reply *pb.CommandReply
}

// Server drives a Store from a raft.Node: it consumes the node's apply
// channel (applying committed commands, installing delivered snapshots)
// and its snapshot trigger (producing and handing back new ones), and
// turns Node.Propose into a synchronous request/reply call for RPC
// handlers to sit on top of. It implements pb.KVServer.
type Server struct {
	pb.UnimplementedKVServer

	node  *raft.Node
	store *Store
	// leaderAddrs, when set, maps node ids to dialable addresses so a
	// redirect can name somewhere the client can actually connect to
	// rather than an identifier it has no way to resolve.
	leaderAddrs map[transport.NodeID]string

	mu      sync.Mutex
	waiters map[uint64]chan applyResult

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// ServerOption customizes a Server.
type ServerOption func(*Server)

// WithLeaderAddresses makes refusals carry the leader's dialable address in
// CommandReply.LeaderHint instead of its bare node id. Without it a client
// that isn't already configured with every node's address has no way to act
// on a redirect. Node ids that aren't in the map fall back to the id.
func WithLeaderAddresses(addrs map[transport.NodeID]string) ServerOption {
	return func(s *Server) {
		s.leaderAddrs = make(map[transport.NodeID]string, len(addrs))
		for id, addr := range addrs {
			s.leaderAddrs[id] = addr
		}
	}
}

// NewServer starts driving store from node's apply channel and snapshot
// trigger. Call Stop to shut down the driving goroutines; it does not stop
// node itself.
func NewServer(node *raft.Node, store *Store, opts ...ServerOption) *Server {
	s := &Server{
		node:    node,
		store:   store,
		waiters: make(map[uint64]chan applyResult),
		stopCh:  make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.wg.Add(1)
	go s.runApplyLoop()
	s.wg.Add(1)
	go s.runSnapshotLoop()
	return s
}

// leaderHint names the current leader in whatever form is most useful to a
// client: an address when one is configured, otherwise the node id.
func (s *Server) leaderHint() string {
	leader := s.node.Leader()
	if leader == "" {
		return ""
	}
	if addr, ok := s.leaderAddrs[leader]; ok {
		return addr
	}
	return string(leader)
}

// Stop halts this Server's driving goroutines and waits for them to exit.
func (s *Server) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
}

// runApplyLoop and runSnapshotLoop select on s.stopCh explicitly rather
// than just ranging over the node's channels, so Server.Stop() can return
// promptly on its own — it must not depend on the underlying raft.Node
// having been stopped first (or at all): callers may stop either in
// either order, and test harnesses that register cleanups for each
// separately will run them LIFO, not necessarily node-then-server.

func (s *Server) runApplyLoop() {
	defer s.wg.Done()
	for {
		select {
		case msg, ok := <-s.node.ApplyCh():
			if !ok {
				return
			}
			if msg.SnapshotValid {
				if err := s.store.Restore(msg.Snapshot); err != nil {
					// The state machine can no longer be trusted to match
					// the rest of the cluster; there's nothing safe to do
					// but stop rather than silently serve wrong answers.
					panic(fmt.Sprintf("kv: restoring snapshot at index %d: %v", msg.SnapshotIndex, err))
				}
				continue
			}
			cmd, reply := s.store.Apply(msg.Command)
			s.deliver(msg.CommandIndex, applyResult{cmd: cmd, reply: reply})
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) runSnapshotLoop() {
	defer s.wg.Done()
	for {
		select {
		case index, ok := <-s.node.SnapshotTrigger():
			if !ok {
				return
			}
			data, err := s.store.Snapshot()
			if err != nil {
				// A snapshot failure here just means we keep growing the
				// log until the next trigger; not fatal, but worth
				// surfacing if this ever gets a logger wired in.
				continue
			}
			_ = s.node.Snapshot(index, data)
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) deliver(index uint64, res applyResult) {
	s.mu.Lock()
	ch, ok := s.waiters[index]
	if ok {
		delete(s.waiters, index)
	}
	s.mu.Unlock()
	if ok {
		ch <- res
	}
}

// execute proposes cmd, waits for whatever command ends up committed at
// the resulting index, and returns its reply — but only if that entry
// really is cmd: if this node lost leadership before cmd committed, a
// different leader's entry may have landed at the same index instead, in
// which case the caller must retry (possibly against a new leader; the
// reply's LeaderHint says who, if known).
func (s *Server) execute(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("kv: encoding command: %w", err)
	}

	index, _, isLeader := s.node.Propose(payload)
	if !isLeader {
		return &pb.CommandReply{Success: false, Error: "not leader", LeaderHint: s.leaderHint()}, nil
	}

	ch := make(chan applyResult, 1)
	s.mu.Lock()
	s.waiters[index] = ch
	s.mu.Unlock()

	select {
	case res := <-ch:
		if res.cmd.ClientId != cmd.ClientId || res.cmd.SeqNo != cmd.SeqNo {
			return &pb.CommandReply{
				Success:    false,
				Error:      "lost leadership before this request committed; retry",
				LeaderHint: s.leaderHint(),
			}, nil
		}
		// Rebuilt rather than annotated in place: for a retried request the
		// store hands back the very reply it cached for the original, and
		// stamping this attempt's index onto it would corrupt the dedup
		// record every later retry is answered from.
		return &pb.CommandReply{
			Success:    res.reply.Success,
			Value:      res.reply.Value,
			Found:      res.reply.Found,
			Error:      res.reply.Error,
			LeaderHint: res.reply.LeaderHint,
			Index:      index,
		}, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return nil, ctx.Err()
	case <-s.stopCh:
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return nil, fmt.Errorf("kv: server stopped")
	}
}

// Get, Put, Append, and Delete implement pb.KVServer. Each forces the
// operation implied by the endpoint itself, regardless of whatever Op the
// client set on the request, so a client can't cross the streams by
// hitting Put with Op_OP_DELETE set.

func (s *Server) Get(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
	cmd.Op = pb.Op_OP_GET
	return s.execute(ctx, cmd)
}

func (s *Server) Put(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
	cmd.Op = pb.Op_OP_PUT
	return s.execute(ctx, cmd)
}

func (s *Server) Append(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
	cmd.Op = pb.Op_OP_APPEND
	return s.execute(ctx, cmd)
}

func (s *Server) Delete(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
	cmd.Op = pb.Op_OP_DELETE
	return s.execute(ctx, cmd)
}
