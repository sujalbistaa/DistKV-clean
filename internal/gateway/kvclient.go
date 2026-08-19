package gateway

// A client, again — internal/shard already has one. This one exists
// because the console's whole point is showing what the client had to do:
// which node it tried, who refused, who redirected it where, and how long
// each hop took. shard.Client deliberately hides all of that behind a
// clean call, which is right for a library and useless for a demo.
//
// The exactly-once contract is the same and matters just as much here: one
// client id for the gateway process, one sequence number per logical
// request, reused across every redirect, so a request that is retried
// against three nodes can still only ever be applied once.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/status"

	"github.com/sujalbistaa/DistKV/internal/transport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

const (
	// attemptTimeout bounds one request to one node. A leader that has
	// been partitioned from its majority still believes it leads: it will
	// accept the request and then wait forever for a commit that can never
	// happen. Without this the client would sit on that node and never try
	// anyone who could actually answer.
	attemptTimeout = 700 * time.Millisecond
	// requestBudget is the total time a browser request gets. An election
	// takes a few hundred milliseconds, so this is several elections'
	// worth: "no leader yet" is a transient state worth waiting through.
	requestBudget = 4 * time.Second
	// retryBackoff paces full passes over the cluster so an in-progress
	// election isn't drowned in retries.
	retryBackoff = 40 * time.Millisecond
)

// Attempt is one hop: the client's contact with a single node.
type Attempt struct {
	Node      string  `json:"node"`
	Outcome   string  `json:"outcome"` // ok, redirected, refused, unreachable
	Detail    string  `json:"detail,omitempty"`
	LatencyMs float64 `json:"latencyMs"`
}

// Result is a completed key-value operation, including the route it took.
type Result struct {
	OK       bool      `json:"ok"`
	Op       string    `json:"op"`
	Key      string    `json:"key"`
	Value    string    `json:"value"`
	Found    bool      `json:"found"`
	ServedBy string    `json:"servedBy,omitempty"`
	Index    uint64    `json:"index,omitempty"`
	Attempts []Attempt `json:"attempts"`
	// TotalMs is wall-clock time for the whole operation, redirects
	// included — what a real client would have waited.
	TotalMs float64 `json:"totalMs"`
	Error   string  `json:"error,omitempty"`
}

// Client issues key-value requests against the cluster, following leader
// redirects and recording the route.
type Client struct {
	cluster  *Cluster
	clientID string
	seq      atomic.Uint64

	mu     sync.Mutex
	leader transport.NodeID // last node known to have served a request
}

// NewClient returns a Client bound to cluster. clientID must be unique to
// this process: the state machine deduplicates writes on (client id,
// sequence number), so sharing one with another client could silently
// suppress a real write.
func NewClient(cluster *Cluster, clientID string) *Client {
	return &Client{cluster: cluster, clientID: clientID}
}

func (c *Client) cachedLeader() transport.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

func (c *Client) setLeader(id transport.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = id
}

func (c *Client) invalidateLeader() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = ""
}

// candidates returns the order to try nodes in: the node believed to be
// leader first, then everyone else. The console's own poll is not
// consulted — the client finds the leader the way any client would, by
// asking, which is what makes the recorded route honest.
func (c *Client) candidates() []transport.NodeID {
	cached := c.cachedLeader()
	members := c.cluster.Members()
	out := make([]transport.NodeID, 0, len(members))
	if cached != "" && c.cluster.Known(cached) {
		out = append(out, cached)
	}
	for _, m := range members {
		if m.ID != cached {
			out = append(out, m.ID)
		}
	}
	return out
}

// Do issues one operation and returns it with its full route. op is one of
// get, put, append, delete.
func (c *Client) Do(ctx context.Context, op, key, value string) Result {
	started := time.Now()
	result := Result{Op: op, Key: key, Attempts: []Attempt{}}

	cmd := &pb.Command{
		ClientId: c.clientID,
		SeqNo:    c.seq.Add(1),
		Key:      key,
		Value:    value,
	}

	ctx, cancel := context.WithTimeout(ctx, requestBudget)
	defer cancel()

	for {
		for _, id := range c.candidates() {
			if ctx.Err() != nil {
				break
			}

			hop := time.Now()
			attemptCtx, cancelAttempt := context.WithTimeout(ctx, attemptTimeout)
			reply, err := c.call(attemptCtx, id, op, cmd)
			cancelAttempt()
			attempt := Attempt{Node: string(id), LatencyMs: msSince(hop)}

			switch {
			case err != nil:
				// Down, cut off, or a leader that can no longer commit.
				// All three look the same from here, which is the point.
				attempt.Outcome = "unreachable"
				attempt.Detail = grpcMessage(err)
				c.invalidateLeader()

			case reply.Success:
				attempt.Outcome = "ok"
				c.setLeader(id)
				result.OK = true
				result.Value = reply.Value
				result.Found = reply.Found
				result.ServedBy = string(id)
				result.Index = reply.Index
				result.Attempts = append(result.Attempts, attempt)
				result.TotalMs = msSince(started)
				return result

			case reply.LeaderHint != "":
				// The node named someone better. The hint is an address so
				// that any client can act on it; the console wants the id.
				target := c.resolveHint(reply.LeaderHint)
				attempt.Outcome = "redirected"
				attempt.Detail = fmt.Sprintf("not leader; sent us to %s", target)
				c.setLeader(target)

			default:
				attempt.Outcome = "refused"
				attempt.Detail = reply.Error
				c.invalidateLeader()
			}

			result.Attempts = append(result.Attempts, attempt)

			// A redirect names exactly who to ask, so start the next pass
			// there rather than walking the rest of this one.
			if attempt.Outcome == "redirected" {
				break
			}
		}

		if ctx.Err() != nil {
			result.TotalMs = msSince(started)
			result.Error = c.explainFailure()
			return result
		}
		select {
		case <-ctx.Done():
		case <-time.After(retryBackoff):
		}
	}
}

// explainFailure turns "nobody answered" into the reason, which the
// current cluster view can almost always supply and which is far more
// useful to read than a timeout.
func (c *Client) explainFailure() string {
	snap := c.cluster.Snapshot()
	if !snap.Cluster.HasQuorum {
		return fmt.Sprintf("no quorum: %d of %d nodes are up, %d are needed to commit anything. "+
			"The store is refusing the write rather than accepting one it could lose.",
			snap.Cluster.Running, snap.Cluster.Size, snap.Cluster.Quorum)
	}
	if snap.Cluster.Leader == "" {
		return "no leader right now: the cluster is mid-election. Retrying would normally succeed within a second."
	}
	return "timed out before any node could commit the request"
}

func (c *Client) resolveHint(hint string) transport.NodeID {
	if id, ok := c.cluster.IDForAddr(hint); ok {
		return id
	}
	// The hint may already be a bare node id, or an address the gateway
	// knows under a different host (a container name vs. a published
	// port). Fall back to matching on the port, then to the id itself.
	if _, port, ok := strings.Cut(hint, ":"); ok {
		for _, m := range c.cluster.Members() {
			if strings.HasSuffix(m.Addr, ":"+port) {
				return m.ID
			}
		}
	}
	return transport.NodeID(hint)
}

func (c *Client) call(ctx context.Context, id transport.NodeID, op string, cmd *pb.Command) (*pb.CommandReply, error) {
	client, ok := c.cluster.kv[id]
	if !ok {
		return nil, fmt.Errorf("unknown node %s", id)
	}
	switch op {
	case "get":
		return client.Get(ctx, cmd)
	case "put":
		return client.Put(ctx, cmd)
	case "append":
		return client.Append(ctx, cmd)
	case "delete":
		return client.Delete(ctx, cmd)
	default:
		return nil, fmt.Errorf("unknown operation %q", op)
	}
}

func msSince(t time.Time) float64 {
	return float64(time.Since(t).Microseconds()) / 1000
}

// grpcMessage strips the gRPC status envelope off an error so the console
// shows "connection refused" rather than a wrapped code and method name.
func grpcMessage(err error) string {
	if err == nil {
		return ""
	}
	if st, ok := status.FromError(err); ok {
		msg := st.Message()
		if i := strings.LastIndex(msg, "desc = "); i >= 0 {
			msg = msg[i+len("desc = "):]
		}
		return msg
	}
	return err.Error()
}
