// Package gateway turns a DistKV cluster into something a browser can
// watch and poke at: it polls every node's Admin service, derives a
// cluster-wide view and a log of what changed, proxies key-value requests
// to whichever node is leader, and exposes the same fault injection the
// test suite uses over HTTP.
//
// It is deliberately the only component that knows about HTTP. The nodes
// themselves speak nothing but gRPC.
package gateway

// Locking: mu guards the latest snapshot, the event ring, and the
// subscriber set. Polling builds a new snapshot without holding it and
// swaps it in at the end, so a slow or unreachable node delays the poll
// but never a reader.

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sujalbistaa/DistKV/internal/transport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// Member is one node the gateway knows about.
type Member struct {
	ID   transport.NodeID
	Addr string
}

// FollowerProgress is a leader's replication bookkeeping for one peer, as
// reported to the browser.
type FollowerProgress struct {
	NextIndex  uint64 `json:"nextIndex"`
	MatchIndex uint64 `json:"matchIndex"`
}

// NodeView is everything the console shows about a single node.
type NodeView struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`

	// Reachable is whether the gateway got an answer at all. A node whose
	// Raft process is gone is unreachable; a node that answered and said
	// it is crashed is reachable but not running. The console distinguishes
	// them because they are genuinely different failures.
	Reachable bool   `json:"reachable"`
	Lifecycle string `json:"lifecycle"` // "running", "crashed", or "unreachable"
	Error     string `json:"error,omitempty"`

	Role         string `json:"role"`
	Term         uint64 `json:"term"`
	LeaderID     string `json:"leaderId"`
	CommitIndex  uint64 `json:"commitIndex"`
	LastApplied  uint64 `json:"lastApplied"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LogLen       uint64 `json:"logLen"`

	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64 `json:"lastIncludedTerm"`

	KeyCount     uint64                      `json:"keyCount"`
	UptimeMs     uint64                      `json:"uptimeMs"`
	IsolatedFrom []string                    `json:"isolatedFrom"`
	Followers    map[string]FollowerProgress `json:"followers,omitempty"`
}

// ClusterView is the derived, whole-cluster summary: the numbers that are
// only meaningful once every node has been asked.
type ClusterView struct {
	Size    int `json:"size"`
	Quorum  int `json:"quorum"`
	Running int `json:"running"`

	// Leader is the node a quorum currently agrees is leader, and Term the
	// term they agree on. A cluster mid-election has no leader, which is a
	// normal state and not an error.
	Leader string `json:"leader"`
	Term   uint64 `json:"term"`

	// HighestTerm is the largest term any node has reached, which during a
	// partition is not the leader's: a cut-off minority keeps timing out
	// and standing for election, driving its own term far above the one
	// the cluster is actually operating in. Watching those two numbers
	// diverge is watching exactly why Raft requires a majority to elect.
	HighestTerm uint64 `json:"highestTerm"`

	// Groups is how many sets of mutually reachable running nodes there
	// are, and LargestGroup the size of the biggest. HasQuorum is whether
	// that biggest group is a majority — the real question, since five
	// running nodes split three ways can commit nothing at all.
	//
	// When HasQuorum is false the store stops accepting writes. That is
	// the guarantee holding, not the system failing.
	Groups       int  `json:"groups"`
	LargestGroup int  `json:"largestGroup"`
	HasQuorum    bool `json:"hasQuorum"`

	CommitIndex uint64 `json:"commitIndex"`
	KeyCount    uint64 `json:"keyCount"`
}

// Snapshot is one poll's worth of cluster state.
type Snapshot struct {
	TS      int64       `json:"ts"`
	Cluster ClusterView `json:"cluster"`
	Nodes   []NodeView  `json:"nodes"`
}

// Event is one notable change, as decided by diffing consecutive
// snapshots, plus the ones the request handlers report themselves.
type Event struct {
	Seq  uint64 `json:"seq"`
	TS   int64  `json:"ts"`
	Kind string `json:"kind"` // election, term, lifecycle, network, write, quorum
	Node string `json:"node,omitempty"`
	Text string `json:"text"`
}

const (
	// pollInterval is how often every node is asked for its status. It is
	// well below the 150-300ms election timeout so an election is several
	// frames long in the console rather than a single flicker.
	pollInterval = 200 * time.Millisecond
	// pollTimeout bounds one node's status call. A node that has stopped
	// answering must not hold up the view of the ones that haven't.
	pollTimeout = 400 * time.Millisecond
	// eventRing is how many recent events are kept for clients that
	// connect mid-stream.
	eventRing = 200
	// termEventInterval is the minimum gap between term-change events for
	// one node. See recordTermAdvance.
	termEventInterval = 4 * time.Second
	// idleBroadcastInterval is how often the view is pushed to subscribers
	// when nothing about it has changed — often enough that a client can
	// tell the stream is alive, rare enough that an idle tab costs almost
	// nothing to keep open.
	idleBroadcastInterval = 3 * time.Second
)

// Cluster polls a set of nodes and maintains the current view of them.
type Cluster struct {
	members []Member
	byID    map[transport.NodeID]Member
	byAddr  map[string]transport.NodeID

	conns map[transport.NodeID]*grpc.ClientConn
	admin map[transport.NodeID]pb.AdminClient
	kv    map[transport.NodeID]pb.KVClient

	mu       sync.Mutex
	snapshot Snapshot
	events   []Event
	nextSeq  uint64
	subs     map[chan struct{}]struct{}
	// lastTermEvent is when each node last had a term change reported, so
	// a node campaigning in a hopeless partition can't flood the log.
	lastTermEvent map[string]time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewCluster dials every member and starts polling. Connections are lazy
// at the gRPC level, so a member that is down at startup is simply
// reported as unreachable until it isn't.
func NewCluster(members []Member) (*Cluster, error) {
	if len(members) == 0 {
		return nil, fmt.Errorf("gateway: at least one node is required")
	}

	c := &Cluster{
		members: append([]Member(nil), members...),
		byID:    make(map[transport.NodeID]Member, len(members)),
		byAddr:  make(map[string]transport.NodeID, len(members)),
		conns:   make(map[transport.NodeID]*grpc.ClientConn, len(members)),
		admin:   make(map[transport.NodeID]pb.AdminClient, len(members)),
		kv:      make(map[transport.NodeID]pb.KVClient, len(members)),
		subs:    make(map[chan struct{}]struct{}),
		stopCh:  make(chan struct{}),
	}
	sort.Slice(c.members, func(i, j int) bool { return c.members[i].ID < c.members[j].ID })

	for _, m := range c.members {
		conn, err := grpc.NewClient(m.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("gateway: dialing %s at %s: %w", m.ID, m.Addr, err)
		}
		c.byID[m.ID] = m
		c.byAddr[m.Addr] = m.ID
		c.conns[m.ID] = conn
		c.admin[m.ID] = pb.NewAdminClient(conn)
		c.kv[m.ID] = pb.NewKVClient(conn)
	}

	c.snapshot = c.poll()
	c.wg.Add(1)
	go c.run()
	return c, nil
}

// Close stops polling and tears down every connection.
func (c *Cluster) Close() {
	c.stopOnce.Do(func() { close(c.stopCh) })
	c.wg.Wait()
	for _, conn := range c.conns {
		conn.Close()
	}
}

// Members returns the configured node set.
func (c *Cluster) Members() []Member { return c.members }

// Quorum is how many nodes must be up for the cluster to commit anything.
func (c *Cluster) Quorum() int { return len(c.members)/2 + 1 }

func (c *Cluster) run() {
	defer c.wg.Done()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	lastBroadcast := time.Now()
	for {
		select {
		case <-ticker.C:
			next := c.poll()
			c.mu.Lock()
			prev := c.snapshot
			c.snapshot = next
			c.mu.Unlock()
			c.recordTransitions(prev, next)

			// Waking every subscriber five times a second to tell them
			// nothing has changed is most of what this would otherwise
			// send: a healthy cluster is in the same state for minutes at
			// a stretch, and an open tab is a connection somebody is
			// paying for the bandwidth of. Idle traffic drops by around
			// twenty times for no loss of responsiveness, because the
			// frames that carry a change are still sent the moment the
			// poll finds it.
			if !materiallyEqual(prev, next) || time.Since(lastBroadcast) >= idleBroadcastInterval {
				lastBroadcast = time.Now()
				c.notify()
			}
		case <-c.stopCh:
			return
		}
	}
}

// poll asks every node for its status concurrently and assembles a
// snapshot. Nodes are always reported in a stable order so the console
// never reshuffles its rows.
func (c *Cluster) poll() Snapshot {
	views := make([]NodeView, len(c.members))
	var wg sync.WaitGroup
	for i, m := range c.members {
		wg.Add(1)
		go func(i int, m Member) {
			defer wg.Done()
			views[i] = c.pollNode(m)
		}(i, m)
	}
	wg.Wait()

	return Snapshot{
		TS:      time.Now().UnixMilli(),
		Cluster: c.summarize(views),
		Nodes:   views,
	}
}

func (c *Cluster) pollNode(m Member) NodeView {
	view := NodeView{ID: string(m.ID), Addr: m.Addr, Lifecycle: "unreachable", IsolatedFrom: []string{}}

	ctx, cancel := context.WithTimeout(context.Background(), pollTimeout)
	defer cancel()

	st, err := c.admin[m.ID].Status(ctx, &pb.StatusRequest{})
	if err != nil {
		view.Error = grpcMessage(err)
		return view
	}

	view.Reachable = true
	view.Lifecycle = "crashed"
	if st.Lifecycle == pb.Lifecycle_LIFECYCLE_RUNNING {
		view.Lifecycle = "running"
	}
	view.Role = st.Role
	view.Term = st.Term
	view.LeaderID = st.LeaderId
	view.CommitIndex = st.CommitIndex
	view.LastApplied = st.LastApplied
	view.LastLogIndex = st.LastLogIndex
	view.LogLen = st.LogLen
	view.LastIncludedIndex = st.LastIncludedIndex
	view.LastIncludedTerm = st.LastIncludedTerm
	view.KeyCount = st.KeyCount
	view.UptimeMs = st.UptimeMs
	if st.IsolatedFrom != nil {
		view.IsolatedFrom = st.IsolatedFrom
	}
	if len(st.Followers) > 0 {
		view.Followers = make(map[string]FollowerProgress, len(st.Followers))
		for peer, p := range st.Followers {
			view.Followers[peer] = FollowerProgress{NextIndex: p.NextIndex, MatchIndex: p.MatchIndex}
		}
	}
	return view
}

// summarize derives the cluster-wide view.
//
// Two things here are less obvious than they look. The leader is only
// reported when a quorum of nodes name the same one in the same term — a
// deposed leader that hasn't heard the news yet still calls itself leader,
// and believing it would show two leaders at once, which Raft's whole
// point is that you never have. And quorum is a question about
// connectivity, not about how many processes are alive: five running nodes
// split into groups of two and three have a quorum, and split into groups
// of two, two, and one have none.
func (c *Cluster) summarize(views []NodeView) ClusterView {
	out := ClusterView{Size: len(views), Quorum: c.Quorum()}

	running := make([]NodeView, 0, len(views))
	for _, v := range views {
		if v.Lifecycle != "running" {
			continue
		}
		running = append(running, v)
		out.Running++
		if v.Term > out.HighestTerm {
			out.HighestTerm = v.Term
		}
		if v.CommitIndex > out.CommitIndex {
			out.CommitIndex = v.CommitIndex
		}
		if v.KeyCount > out.KeyCount {
			out.KeyCount = v.KeyCount
		}
	}

	// Whoever a quorum agrees on, in the highest term that has such an
	// agreement. Ordering by term matters during recovery from a
	// partition, when nodes briefly still name the previous leader.
	agreement := make(map[[2]string]int)
	terms := make(map[[2]string]uint64)
	for _, v := range running {
		if v.LeaderID == "" {
			continue
		}
		key := [2]string{v.LeaderID, fmt.Sprint(v.Term)}
		agreement[key]++
		terms[key] = v.Term
	}
	for key, votes := range agreement {
		if votes >= out.Quorum && terms[key] >= out.Term {
			out.Leader, out.Term = key[0], terms[key]
		}
	}

	out.Groups, out.LargestGroup = connectivity(running)
	out.HasQuorum = out.LargestGroup >= out.Quorum
	return out
}

// connectivity groups the running nodes into sets that can still reach one
// another and returns how many groups there are and how big the biggest
// is. Two nodes are connected when neither has been cut off from the
// other, which is the same condition the transport itself applies.
func connectivity(running []NodeView) (groups, largest int) {
	cutOff := make(map[string]map[string]bool, len(running))
	for _, v := range running {
		set := make(map[string]bool, len(v.IsolatedFrom))
		for _, peer := range v.IsolatedFrom {
			set[peer] = true
		}
		cutOff[v.ID] = set
	}
	connected := func(a, b string) bool {
		return !cutOff[a][b] && !cutOff[b][a]
	}

	seen := make(map[string]bool, len(running))
	for _, start := range running {
		if seen[start.ID] {
			continue
		}
		groups++
		size := 0
		// Breadth-first over the reachability graph. Reachability is not
		// transitive under arbitrary partitions — A may reach B and B
		// reach C while A cannot reach C — so this is a lower bound on
		// how badly split the cluster is, and a group it reports is at
		// least connected enough to gossip.
		queue := []string{start.ID}
		seen[start.ID] = true
		for len(queue) > 0 {
			id := queue[0]
			queue = queue[1:]
			size++
			for _, other := range running {
				if !seen[other.ID] && connected(id, other.ID) {
					seen[other.ID] = true
					queue = append(queue, other.ID)
				}
			}
		}
		if size > largest {
			largest = size
		}
	}
	return groups, largest
}

// recordTransitions turns the difference between two snapshots into the
// event log the console shows. Only state changes are recorded; the
// indices that advance on every write would drown everything else.
func (c *Cluster) recordTransitions(prev, next Snapshot) {
	before := make(map[string]NodeView, len(prev.Nodes))
	for _, v := range prev.Nodes {
		before[v.ID] = v
	}

	for _, now := range next.Nodes {
		was, ok := before[now.ID]
		if !ok {
			continue
		}

		if was.Lifecycle != now.Lifecycle {
			switch now.Lifecycle {
			case "crashed":
				c.Record("lifecycle", now.ID, fmt.Sprintf("%s crashed — process state gone, disk intact", now.ID))
			case "unreachable":
				c.Record("lifecycle", now.ID, fmt.Sprintf("%s stopped answering", now.ID))
			case "running":
				c.Record("lifecycle", now.ID, fmt.Sprintf("%s recovered from its write-ahead log", now.ID))
			}
		}

		if now.Lifecycle == "running" {
			if was.Role != now.Role && now.Role == "leader" {
				c.Record("election", now.ID, fmt.Sprintf("%s won the election for term %d", now.ID, now.Term))
			}
			if was.Role == "leader" && now.Role != "leader" && was.Lifecycle == "running" {
				c.Record("election", now.ID, fmt.Sprintf("%s stepped down to %s", now.ID, now.Role))
			}
			if now.Term > was.Term && was.Lifecycle == "running" {
				c.recordTermAdvance(now, now.Term-was.Term)
			}
		}

		if !sameStrings(was.IsolatedFrom, now.IsolatedFrom) {
			if len(now.IsolatedFrom) == 0 {
				c.Record("network", now.ID, fmt.Sprintf("%s rejoined the network", now.ID))
			} else {
				c.Record("network", now.ID, fmt.Sprintf("%s partitioned away from %v", now.ID, now.IsolatedFrom))
			}
		}
	}

	if prev.Cluster.HasQuorum != next.Cluster.HasQuorum {
		if next.Cluster.HasQuorum {
			c.Record("quorum", "", fmt.Sprintf("quorum restored — %d of %d nodes up", next.Cluster.Running, next.Cluster.Size))
		} else {
			c.Record("quorum", "", fmt.Sprintf("quorum lost — %d of %d nodes up, %d needed; writes now correctly refused",
				next.Cluster.Running, next.Cluster.Size, next.Cluster.Quorum))
		}
	}
}

// recordTermAdvance reports a node's term going up — but most term changes
// are not worth a line. Two filters, both about signal:
//
// A follower stepping up by one has simply adopted the term of whoever
// just won, which the election event immediately above it already said; on
// a five-node cluster that is four redundant lines per failover. A jump of
// more than one is different, because it means this node just met someone
// carrying a term far ahead of its own — which is what a node returning
// from a partition does to a cluster, and worth seeing.
//
// And a node cut off from the majority campaigns several times a second
// forever, so its bumps are throttled: what matters there is not that the
// term changed but that this node keeps standing and keeps not winning.
func (c *Cluster) recordTermAdvance(node NodeView, jump uint64) {
	if node.Role != "candidate" && jump <= 1 {
		return
	}

	c.mu.Lock()
	last, seen := c.lastTermEvent[node.ID]
	throttled := seen && time.Since(last) < termEventInterval
	if !throttled {
		if c.lastTermEvent == nil {
			c.lastTermEvent = make(map[string]time.Time)
		}
		c.lastTermEvent[node.ID] = time.Now()
	}
	c.mu.Unlock()

	if throttled {
		return
	}
	if node.Role == "candidate" {
		c.Record("term", node.ID, fmt.Sprintf(
			"%s is standing for election and losing — now at term %d, with no majority to elect it",
			node.ID, node.Term))
		return
	}
	c.Record("term", node.ID, fmt.Sprintf(
		"%s jumped to term %d — it has met a node that has been electing without it", node.ID, node.Term))
}

// materiallyEqual reports whether two snapshots say the same thing about
// the cluster. Two fields are deliberately ignored: the timestamp, which
// differs on every poll by construction, and each node's uptime, which
// counts up continuously and is not shown anywhere — treating either as a
// change would mean nothing ever compares equal and the whole exercise
// would be pointless.
func materiallyEqual(a, b Snapshot) bool {
	if a.Cluster != b.Cluster || len(a.Nodes) != len(b.Nodes) {
		return false
	}
	for i := range a.Nodes {
		if !nodeViewEqual(a.Nodes[i], b.Nodes[i]) {
			return false
		}
	}
	return true
}

func nodeViewEqual(a, b NodeView) bool {
	if a.ID != b.ID ||
		a.Addr != b.Addr ||
		a.Reachable != b.Reachable ||
		a.Lifecycle != b.Lifecycle ||
		a.Error != b.Error ||
		a.Role != b.Role ||
		a.Term != b.Term ||
		a.LeaderID != b.LeaderID ||
		a.CommitIndex != b.CommitIndex ||
		a.LastApplied != b.LastApplied ||
		a.LastLogIndex != b.LastLogIndex ||
		a.LogLen != b.LogLen ||
		a.LastIncludedIndex != b.LastIncludedIndex ||
		a.LastIncludedTerm != b.LastIncludedTerm ||
		a.KeyCount != b.KeyCount {
		return false
	}
	if !sameStrings(a.IsolatedFrom, b.IsolatedFrom) || len(a.Followers) != len(b.Followers) {
		return false
	}
	for peer, progress := range a.Followers {
		if other, ok := b.Followers[peer]; !ok || other != progress {
			return false
		}
	}
	return true
}

// Record appends an event to the ring and wakes every subscriber.
func (c *Cluster) Record(kind, node, text string) {
	c.mu.Lock()
	c.nextSeq++
	c.events = append(c.events, Event{
		Seq:  c.nextSeq,
		TS:   time.Now().UnixMilli(),
		Kind: kind,
		Node: node,
		Text: text,
	})
	if len(c.events) > eventRing {
		c.events = append([]Event(nil), c.events[len(c.events)-eventRing:]...)
	}
	c.mu.Unlock()
	c.notify()
}

// Snapshot returns the most recent poll.
func (c *Cluster) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshot
}

// EventsSince returns every event recorded after seq, oldest first.
func (c *Cluster) EventsSince(seq uint64) []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Event, 0, len(c.events))
	for _, e := range c.events {
		if e.Seq > seq {
			out = append(out, e)
		}
	}
	return out
}

// Subscribe returns a channel that receives a value whenever the view
// changes, and a function to unsubscribe. The channel is coalescing: a
// slow consumer sees that something changed, not every change.
func (c *Cluster) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	c.mu.Lock()
	c.subs[ch] = struct{}{}
	c.mu.Unlock()
	return ch, func() {
		c.mu.Lock()
		delete(c.subs, ch)
		c.mu.Unlock()
	}
}

func (c *Cluster) notify() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for ch := range c.subs {
		select {
		case ch <- struct{}{}:
		default: // already pending; the subscriber will see the latest state anyway
		}
	}
}

// IDForAddr maps a dialable address back to a node id, which is how a
// leader redirect (which names an address, so that any client can act on
// it) is turned back into something the console can highlight.
func (c *Cluster) IDForAddr(addr string) (transport.NodeID, bool) {
	id, ok := c.byAddr[addr]
	return id, ok
}

// Known reports whether id is a member of the cluster.
func (c *Cluster) Known(id transport.NodeID) bool {
	_, ok := c.byID[id]
	return ok
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
