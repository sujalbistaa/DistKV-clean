// Package supervisor owns the lifecycle of one DistKV node: the
// raft.Node, the kv.Store it drives, and the disk storage underneath
// them. Holding all three behind one object is what lets a node be
// destroyed and rebuilt in place — a crash and a recovery — while the
// process, its gRPC listener, and its peer connections stay up.
//
// It is also the node's gRPC surface. It implements pb.KVServer by
// forwarding to whatever incarnation is current, and pb.AdminServer by
// reporting on and manipulating that incarnation.
package supervisor

// Locking: mu guards the current incarnation (node, store, kvServer,
// storage, startedAt, crashed) and is held only long enough to swap it or
// read a pointer out of it. KV requests grab the pointer they need and
// release mu before calling into it, so a request blocked waiting for a
// log entry to commit can never delay a crash or a status poll.

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sujalbistaa/DistKV/internal/kv"
	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/storage"
	"github.com/sujalbistaa/DistKV/internal/transport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// Network is the transport a supervisor drives: the usual RPC plumbing
// plus the ability to cut this node off from named peers. The concrete
// implementation is grpctransport.Transport; the interface exists so the
// supervisor can be tested without a real listener.
type Network interface {
	transport.Transport
	SetIsolation(peers []transport.NodeID)
	Isolation() []transport.NodeID
}

// Config describes the node a Supervisor should run. Everything in it
// survives a crash — it is exactly what New would be handed again if the
// process were restarted from scratch.
type Config struct {
	ID        transport.NodeID
	Peers     []transport.NodeID          // the rest of the Raft group, excluding this node
	Addrs     map[transport.NodeID]string // every node's dialable address, for leader redirects
	DataDir   string                      // where the write-ahead log and snapshots live
	Transport Network

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	SnapshotThreshold  int
	Logger             *log.Logger
}

// Supervisor runs one node and exposes it over gRPC.
type Supervisor struct {
	pb.UnimplementedKVServer
	pb.UnimplementedAdminServer

	cfg Config

	mu        sync.Mutex
	node      *raft.Node
	store     *kv.Store
	kvServer  *kv.Server
	storage   *storage.DiskStorage
	startedAt time.Time
	crashed   bool
}

// New starts a node from whatever is already durable in cfg.DataDir and
// returns it running.
func New(cfg Config) (*Supervisor, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("supervisor: Config.ID is required")
	}
	if cfg.Transport == nil {
		return nil, fmt.Errorf("supervisor: Config.Transport is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("supervisor: Config.DataDir is required")
	}

	s := &Supervisor{cfg: cfg}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.startLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

// startLocked builds a fresh incarnation of the node from disk. It is the
// only place a raft.Node is constructed, so a restart and a cold process
// start take precisely the same path.
func (s *Supervisor) startLocked() error {
	store, err := storage.NewDiskStorage(s.cfg.DataDir)
	if err != nil {
		return fmt.Errorf("supervisor: opening storage: %w", err)
	}

	node, err := raft.NewNode(raft.Config{
		ID:                 s.cfg.ID,
		Peers:              s.cfg.Peers,
		Transport:          s.cfg.Transport,
		Storage:            store,
		ElectionTimeoutMin: s.cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: s.cfg.ElectionTimeoutMax,
		HeartbeatInterval:  s.cfg.HeartbeatInterval,
		SnapshotThreshold:  s.cfg.SnapshotThreshold,
		Seed:               time.Now().UnixNano(),
		Logger:             s.cfg.Logger,
	})
	if err != nil {
		store.Close()
		return fmt.Errorf("supervisor: starting raft node: %w", err)
	}

	kvStore := kv.NewStore()
	s.node = node
	s.store = kvStore
	s.kvServer = kv.NewServer(node, kvStore, kv.WithLeaderAddresses(s.cfg.Addrs))
	s.storage = store
	s.startedAt = time.Now()
	s.crashed = false
	return nil
}

// stopLocked tears the current incarnation down. Order matters: the
// transport handler goes first so no inbound RPC can reach a node that is
// on its way out, and storage is closed last, once every goroutine that
// might write to it has been waited for.
//
// A client request already inside Propose when this runs may still attempt
// a write against the closing storage. That write fails rather than tears
// anything: raft.Node treats a storage error by disabling itself, and the
// node it would be disabling is the one being destroyed anyway.
func (s *Supervisor) stopLocked() {
	if s.node == nil {
		return
	}
	s.cfg.Transport.RegisterHandler(nil)
	s.kvServer.Stop()
	s.node.Stop()
	s.storage.Close()
	s.node, s.store, s.kvServer, s.storage = nil, nil, nil, nil
}

// Close stops the node for good. It does not close the transport.
func (s *Supervisor) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopLocked()
	s.crashed = true
}

// crash destroys the node the way a kill -9 would: every goroutine gone,
// every file handle closed, nothing on disk touched. The process stays up
// only so that a restart — and Status, which keeps reporting the node as
// crashed — remain reachable.
func (s *Supervisor) crash() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.crashed {
		return
	}
	s.stopLocked()
	s.crashed = true
}

// restart rebuilds the node from its write-ahead log and most recent
// snapshot. Whatever it had durably committed before the crash it has
// again; whatever it had merely acknowledged in memory it does not.
func (s *Supervisor) restart() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.crashed {
		return nil
	}
	return s.startLocked()
}

// current returns the live KV server, or nil if the node is crashed.
func (s *Supervisor) current() *kv.Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kvServer
}

// Node returns the running raft.Node, or nil if the node is crashed.
func (s *Supervisor) Node() *raft.Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.node
}

// errCrashed is what a client sees when it reaches a node that isn't
// running: the same Unavailable it would get from a process that really
// had died, so a client's retry logic needs no special case for it.
func errCrashed(id transport.NodeID) error {
	return status.Errorf(codes.Unavailable, "distkv: node %s is down", id)
}

// Get, Put, Append, and Delete implement pb.KVServer by forwarding to the
// current incarnation.

func (s *Supervisor) Get(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
	server := s.current()
	if server == nil {
		return nil, errCrashed(s.cfg.ID)
	}
	return server.Get(ctx, cmd)
}

func (s *Supervisor) Put(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
	server := s.current()
	if server == nil {
		return nil, errCrashed(s.cfg.ID)
	}
	return server.Put(ctx, cmd)
}

func (s *Supervisor) Append(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
	server := s.current()
	if server == nil {
		return nil, errCrashed(s.cfg.ID)
	}
	return server.Append(ctx, cmd)
}

func (s *Supervisor) Delete(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
	server := s.current()
	if server == nil {
		return nil, errCrashed(s.cfg.ID)
	}
	return server.Delete(ctx, cmd)
}

// Status, SetPartition, Crash, and Restart implement pb.AdminServer.

func (s *Supervisor) Status(ctx context.Context, _ *pb.StatusRequest) (*pb.NodeStatus, error) {
	return s.status(), nil
}

func (s *Supervisor) SetPartition(ctx context.Context, req *pb.SetPartitionRequest) (*pb.NodeStatus, error) {
	peers := make([]transport.NodeID, 0, len(req.Peers))
	for _, peer := range req.Peers {
		peers = append(peers, transport.NodeID(peer))
	}
	s.cfg.Transport.SetIsolation(peers)
	return s.status(), nil
}

func (s *Supervisor) Crash(ctx context.Context, _ *pb.LifecycleRequest) (*pb.LifecycleReply, error) {
	s.crash()
	return &pb.LifecycleReply{Lifecycle: pb.Lifecycle_LIFECYCLE_CRASHED}, nil
}

func (s *Supervisor) Restart(ctx context.Context, _ *pb.LifecycleRequest) (*pb.LifecycleReply, error) {
	if err := s.restart(); err != nil {
		return &pb.LifecycleReply{Lifecycle: pb.Lifecycle_LIFECYCLE_CRASHED, Error: err.Error()}, nil
	}
	return &pb.LifecycleReply{Lifecycle: pb.Lifecycle_LIFECYCLE_RUNNING}, nil
}

// status renders the current incarnation as a wire status. A crashed node
// reports its identity, its isolation, and nothing else — there is no
// consensus state to report, and reporting stale numbers would misrepresent
// a node that is genuinely gone.
func (s *Supervisor) status() *pb.NodeStatus {
	isolated := s.cfg.Transport.Isolation()
	isolatedFrom := make([]string, 0, len(isolated))
	for _, peer := range isolated {
		isolatedFrom = append(isolatedFrom, string(peer))
	}

	s.mu.Lock()
	node, store, startedAt := s.node, s.store, s.startedAt
	s.mu.Unlock()

	out := &pb.NodeStatus{
		Id:           string(s.cfg.ID),
		Lifecycle:    pb.Lifecycle_LIFECYCLE_CRASHED,
		IsolatedFrom: isolatedFrom,
	}
	if node == nil {
		return out
	}

	st := node.Status()
	followers := make(map[string]*pb.FollowerProgress, len(st.Followers))
	for peer, progress := range st.Followers {
		followers[string(peer)] = &pb.FollowerProgress{
			NextIndex:  progress.NextIndex,
			MatchIndex: progress.MatchIndex,
		}
	}

	out.Lifecycle = pb.Lifecycle_LIFECYCLE_RUNNING
	out.Role = st.Role.String()
	out.Term = st.Term
	out.LeaderId = string(st.LeaderID)
	out.CommitIndex = st.CommitIndex
	out.LastApplied = st.LastApplied
	out.LastLogIndex = st.LastLogIndex
	out.LogLen = uint64(st.LogLen)
	out.LastIncludedIndex = st.LastIncludedIndex
	out.LastIncludedTerm = st.LastIncludedTerm
	out.Followers = followers
	out.KeyCount = uint64(store.Len())
	out.UptimeMs = uint64(time.Since(startedAt).Milliseconds())
	return out
}
