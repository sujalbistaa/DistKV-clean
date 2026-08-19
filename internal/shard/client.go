package shard

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sujalbistaa/DistKV/internal/transport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// KV is the subset of the KV API a client needs from one node. kv.Server
// satisfies it directly for in-process use; a gRPC connection satisfies it
// through a thin adapter that drops the variadic call options.
type KV interface {
	Get(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error)
	Put(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error)
	Append(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error)
	Delete(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error)
}

// Resolver hands back a KV handle for a node, dialing or reusing a
// connection as it sees fit.
type Resolver interface {
	Resolve(node transport.NodeID) (KV, error)
}

const (
	// defaultAttemptTimeout bounds a single request to a single node. It
	// matters more than it looks: a leader that has been partitioned away
	// from its majority still believes it leads, so it will accept a
	// request and then block indefinitely waiting for a commit that can
	// never happen. Without a per-attempt deadline, one such node would
	// consume the caller's entire budget and the client would never try
	// anyone else.
	defaultAttemptTimeout = 500 * time.Millisecond
	// defaultRetryBackoff paces full passes over a shard's replicas, so a
	// shard with no leader at all (mid-election, or majority lost) is
	// retried steadily rather than in a hot loop.
	defaultRetryBackoff = 20 * time.Millisecond
)

// Client routes requests to the right shard and that shard's leader. It
// generates the monotonic sequence numbers the KV state machine needs for
// exactly-once semantics, so a retry it performs internally after a
// redirect carries the same (client id, seq no) as the attempt it's
// replacing and can never apply twice.
//
// Locking: seq is atomic; the leader cache lives in Router, which guards
// itself. Client holds no other mutable state.
type Client struct {
	router         *Router
	resolver       Resolver
	clientID       string
	attemptTimeout time.Duration
	retryBackoff   time.Duration
	seq            atomic.Uint64
}

// Option customizes a Client.
type Option func(*Client)

// WithAttemptTimeout bounds how long a single attempt against a single
// node may take before the client gives up on it and tries the next.
func WithAttemptTimeout(d time.Duration) Option {
	return func(c *Client) { c.attemptTimeout = d }
}

// WithRetryBackoff sets how long the client waits after exhausting a
// shard's replicas before trying them again.
func WithRetryBackoff(d time.Duration) Option {
	return func(c *Client) { c.retryBackoff = d }
}

// NewClient returns a Client that routes through router and reaches nodes
// through resolver. clientID must be unique per client session — the KV
// state machine deduplicates on (clientID, seqNo).
func NewClient(router *Router, resolver Resolver, clientID string, opts ...Option) *Client {
	c := &Client{
		router:         router,
		resolver:       resolver,
		clientID:       clientID,
		attemptTimeout: defaultAttemptTimeout,
		retryBackoff:   defaultRetryBackoff,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	reply, err := c.do(ctx, key, func(kv KV, ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
		return kv.Get(ctx, cmd)
	}, "")
	if err != nil {
		return "", false, err
	}
	return reply.Value, reply.Found, nil
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, key, func(kv KV, ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
		return kv.Put(ctx, cmd)
	}, value)
	return err
}

func (c *Client) Append(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, key, func(kv KV, ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
		return kv.Append(ctx, cmd)
	}, value)
	return err
}

func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.do(ctx, key, func(kv KV, ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error) {
		return kv.Delete(ctx, cmd)
	}, "")
	return err
}

type callFunc func(kv KV, ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error)

// do resolves key's shard and sends the request to its leader, following
// redirect hints and falling back through the shard's other replicas. It
// keeps trying until ctx is done, since "no leader right now" is a normal,
// transient state during an election rather than a permanent failure.
func (c *Client) do(ctx context.Context, key string, call callFunc, value string) (*pb.CommandReply, error) {
	s, err := c.router.ShardFor(key)
	if err != nil {
		return nil, err
	}

	// One sequence number per logical request, reused across every
	// internal retry below so the state machine can deduplicate them.
	cmd := &pb.Command{
		ClientId: c.clientID,
		SeqNo:    c.seq.Add(1),
		Key:      key,
		Value:    value,
	}

	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("shard: shard %d unavailable: %w", s.ID, lastErr)
			}
			return nil, err
		}

		for _, node := range c.router.Candidates(s) {
			if ctx.Err() != nil {
				break
			}

			kv, err := c.resolver.Resolve(node)
			if err != nil {
				lastErr = err
				continue
			}

			attemptCtx, cancel := context.WithTimeout(ctx, c.attemptTimeout)
			reply, err := call(kv, attemptCtx, cmd)
			cancel()
			if err != nil {
				// Unreachable, timed out, or refused: this node is not a
				// usable leader right now. Note that a leader cut off from
				// its majority looks exactly like this — it accepts the
				// request and then never commits it — which is why the
				// attempt above is separately bounded.
				c.router.InvalidateLeader(s.ID)
				lastErr = err
				continue
			}
			if reply.Success {
				c.router.SetLeader(s.ID, node)
				return reply, nil
			}

			// Refused. If it named a leader, believe it and go straight
			// there on the next pass; otherwise drop what we cached.
			if reply.LeaderHint != "" {
				c.router.SetLeader(s.ID, transport.NodeID(reply.LeaderHint))
			} else {
				c.router.InvalidateLeader(s.ID)
			}
			lastErr = fmt.Errorf("%s", reply.Error)
		}

		// Every replica refused or was unreachable: the shard has no
		// leader we can reach right now. Wait a moment before sweeping
		// them again, so an in-progress election isn't drowned in
		// retries.
		select {
		case <-ctx.Done():
		case <-time.After(c.retryBackoff):
		}
	}
}
