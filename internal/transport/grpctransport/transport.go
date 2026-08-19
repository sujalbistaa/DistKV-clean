// Package grpctransport implements transport.Transport over gRPC: the
// production counterpart to the in-memory fake network that tests use.
//
// The consensus code never sees protobuf. This package is the only place
// that knows how to turn a raft.RequestVoteArgs into a wire message and
// back, which is what lets the same Raft implementation run unchanged over
// either transport.
package grpctransport

// Locking: mu guards handler and the conns cache. Dialing happens under
// mu, but RPCs are always issued with mu released, so a slow or
// unreachable peer never blocks traffic to the others.

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/transport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// Transport is a transport.Transport that reaches peers over gRPC. It is
// also the pb.RaftServer for inbound traffic: register it with a
// grpc.Server to receive RPCs.
type Transport struct {
	pb.UnimplementedRaftServer

	id    transport.NodeID
	peers map[transport.NodeID]string // node id -> host:port

	mu      sync.Mutex
	handler transport.Handler
	conns   map[transport.NodeID]*grpc.ClientConn
	// isolated is the set of peers this transport currently refuses Raft
	// traffic with, in both directions — the production counterpart of
	// Network.Partition in the fake transport. It is empty in normal
	// operation and only ever set by fault injection.
	isolated map[transport.NodeID]bool
}

var _ transport.Transport = (*Transport)(nil)

// New returns a Transport bound to id that dials peers by the addresses in
// peers. Connections are established lazily and reused.
func New(id transport.NodeID, peers map[transport.NodeID]string) *Transport {
	addrs := make(map[transport.NodeID]string, len(peers))
	for peer, addr := range peers {
		addrs[peer] = addr
	}
	return &Transport{
		id:       id,
		peers:    addrs,
		conns:    make(map[transport.NodeID]*grpc.ClientConn),
		isolated: make(map[transport.NodeID]bool),
	}
}

// LocalID returns this transport's node id.
func (t *Transport) LocalID() transport.NodeID { return t.id }

// RegisterHandler installs the function invoked for inbound RPCs.
func (t *Transport) RegisterHandler(h transport.Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = h
}

// SetIsolation replaces the set of peers this node refuses Raft traffic
// with. Both directions are cut: sends to an isolated peer fail with
// transport.ErrUnreachable, and inbound RPCs from one are rejected before
// they reach the consensus code. Passing an empty set heals the node.
//
// This is fault injection, not membership change: an isolated peer is
// still a voting member of the cluster that simply cannot be reached, so
// the rest of the cluster keeps counting it towards the quorum it needs.
// Only Raft traffic is affected — the node stays reachable on every other
// service registered alongside this one, which is what lets a partitioned
// node still be observed and healed.
func (t *Transport) SetIsolation(peers []transport.NodeID) {
	isolated := make(map[transport.NodeID]bool, len(peers))
	for _, peer := range peers {
		if peer != t.id {
			isolated[peer] = true
		}
	}
	t.mu.Lock()
	t.isolated = isolated
	t.mu.Unlock()
}

// Isolation returns the peers currently cut off from this node, sorted.
func (t *Transport) Isolation() []transport.NodeID {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]transport.NodeID, 0, len(t.isolated))
	for peer := range t.isolated {
		out = append(out, peer)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (t *Transport) isolatedFrom(peer transport.NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.isolated[peer]
}

// Close tears down every open peer connection.
func (t *Transport) Close() error {
	t.mu.Lock()
	conns := t.conns
	t.conns = make(map[transport.NodeID]*grpc.ClientConn)
	t.mu.Unlock()

	var firstErr error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// client returns a cached connection to peer, dialing if necessary.
// grpc.NewClient does not block on connectivity, so this is cheap and a
// down peer simply produces errors on use rather than stalling here.
func (t *Transport) client(peer transport.NodeID) (pb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[peer]; ok {
		return pb.NewRaftClient(conn), nil
	}
	addr, ok := t.peers[peer]
	if !ok {
		return nil, fmt.Errorf("grpctransport: no address configured for peer %s", peer)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpctransport: dialing %s at %s: %w", peer, addr, err)
	}
	t.conns[peer] = conn
	return pb.NewRaftClient(conn), nil
}

// Send delivers req to peer and returns its reply, converting between the
// raft package's Go types and their wire form.
func (t *Transport) Send(ctx context.Context, to transport.NodeID, req any) (any, error) {
	if t.isolatedFrom(to) {
		return nil, transport.ErrUnreachable
	}
	client, err := t.client(to)
	if err != nil {
		return nil, err
	}

	switch args := req.(type) {
	case *raft.RequestVoteArgs:
		resp, err := client.RequestVote(ctx, &pb.RequestVoteRequest{
			Term:         args.Term,
			CandidateId:  string(args.CandidateID),
			LastLogIndex: args.LastLogIndex,
			LastLogTerm:  args.LastLogTerm,
		})
		if err != nil {
			return nil, err
		}
		return &raft.RequestVoteReply{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil

	case *raft.AppendEntriesArgs:
		resp, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
			Term:         args.Term,
			LeaderId:     string(args.LeaderID),
			PrevLogIndex: args.PrevLogIndex,
			PrevLogTerm:  args.PrevLogTerm,
			Entries:      entriesToProto(args.Entries),
			LeaderCommit: args.LeaderCommit,
		})
		if err != nil {
			return nil, err
		}
		return &raft.AppendEntriesReply{
			Term:          resp.Term,
			Success:       resp.Success,
			ConflictTerm:  resp.ConflictTerm,
			ConflictIndex: resp.ConflictIndex,
		}, nil

	case *raft.InstallSnapshotArgs:
		resp, err := client.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{
			Term:              args.Term,
			LeaderId:          string(args.LeaderID),
			LastIncludedIndex: args.LastIncludedIndex,
			LastIncludedTerm:  args.LastIncludedTerm,
			Data:              args.Data,
		})
		if err != nil {
			return nil, err
		}
		return &raft.InstallSnapshotReply{Term: resp.Term}, nil

	default:
		return nil, fmt.Errorf("grpctransport: unknown request type %T", req)
	}
}

// dispatch hands an inbound request to the registered handler.
func (t *Transport) dispatch(ctx context.Context, from transport.NodeID, req any) (any, error) {
	t.mu.Lock()
	handler, isolated := t.handler, t.isolated[from]
	t.mu.Unlock()
	// Dropping an inbound RPC from an isolated peer rather than answering
	// it is what makes the partition symmetric: the sender was able to
	// establish a TCP connection, but its message never reaches consensus.
	if isolated {
		return nil, transport.ErrUnreachable
	}
	if handler == nil {
		return nil, fmt.Errorf("grpctransport: no handler registered on %s", t.id)
	}
	return handler(ctx, from, req)
}

// RequestVote implements pb.RaftServer.
func (t *Transport) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	reply, err := t.dispatch(ctx, transport.NodeID(req.CandidateId), &raft.RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  transport.NodeID(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	r := reply.(*raft.RequestVoteReply)
	return &pb.RequestVoteResponse{Term: r.Term, VoteGranted: r.VoteGranted}, nil
}

// AppendEntries implements pb.RaftServer.
func (t *Transport) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	reply, err := t.dispatch(ctx, transport.NodeID(req.LeaderId), &raft.AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     transport.NodeID(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entriesFromProto(req.Entries),
		LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	r := reply.(*raft.AppendEntriesReply)
	return &pb.AppendEntriesResponse{
		Term:          r.Term,
		Success:       r.Success,
		ConflictTerm:  r.ConflictTerm,
		ConflictIndex: r.ConflictIndex,
	}, nil
}

// InstallSnapshot implements pb.RaftServer.
func (t *Transport) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	reply, err := t.dispatch(ctx, transport.NodeID(req.LeaderId), &raft.InstallSnapshotArgs{
		Term:              req.Term,
		LeaderID:          transport.NodeID(req.LeaderId),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})
	if err != nil {
		return nil, err
	}
	return &pb.InstallSnapshotResponse{Term: reply.(*raft.InstallSnapshotReply).Term}, nil
}

func entriesToProto(entries []raft.LogEntry) []*pb.LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]*pb.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = &pb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
	}
	return out
}

func entriesFromProto(entries []*pb.LogEntry) []raft.LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]raft.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = raft.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
	}
	return out
}
