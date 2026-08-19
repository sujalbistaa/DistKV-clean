package gateway

// The gateway's own reasoning, tested apart from any cluster: how it
// decides who the leader is and whether a quorum still exists. Both
// answers are derived from a pile of individually honest node reports that
// can disagree with one another, and getting either wrong would make the
// console lie about a system that isn't lying.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/internal/transport"
)

func newTestCluster(ids ...string) *Cluster {
	members := make([]Member, len(ids))
	for i, id := range ids {
		members[i] = Member{ID: transport.NodeID(id), Addr: id + ":7070"}
	}
	return &Cluster{members: members}
}

func running(id, role string, term uint64, leader string, isolated ...string) NodeView {
	if isolated == nil {
		isolated = []string{}
	}
	return NodeView{
		ID:           id,
		Lifecycle:    "running",
		Reachable:    true,
		Role:         role,
		Term:         term,
		LeaderID:     leader,
		IsolatedFrom: isolated,
	}
}

func TestSummarizeNamesTheLeaderAQuorumAgreesOn(t *testing.T) {
	c := newTestCluster("a", "b", "c", "d", "e")

	view := c.summarize([]NodeView{
		running("a", "leader", 4, "a"),
		running("b", "follower", 4, "a"),
		running("c", "follower", 4, "a"),
		running("d", "follower", 4, "a"),
		running("e", "follower", 4, "a"),
	})

	require.Equal(t, "a", view.Leader)
	require.Equal(t, uint64(4), view.Term)
	require.True(t, view.HasQuorum)
	require.Equal(t, 1, view.Groups)
}

// A leader that has been cut off from the cluster goes on calling itself
// leader until it hears a higher term. Reporting it would show two leaders
// at once — the one thing Raft guarantees cannot happen — so the summary
// believes the quorum, not the node.
func TestSummarizeIgnoresADeposedLeaderStillClaimingTheTitle(t *testing.T) {
	c := newTestCluster("a", "b", "c", "d", "e")

	view := c.summarize([]NodeView{
		running("a", "leader", 4, "a", "b", "c", "d", "e"), // stale, cut off
		running("b", "leader", 5, "b", "a"),
		running("c", "follower", 5, "b", "a"),
		running("d", "follower", 5, "b", "a"),
		running("e", "follower", 5, "b", "a"),
	})

	require.Equal(t, "b", view.Leader, "the quorum's leader, not the loudest node")
	require.Equal(t, uint64(5), view.Term)
	require.True(t, view.HasQuorum)
	require.Equal(t, 2, view.Groups)
	require.Equal(t, 4, view.LargestGroup)
}

func TestSummarizeReportsNoLeaderDuringAnElection(t *testing.T) {
	c := newTestCluster("a", "b", "c")

	view := c.summarize([]NodeView{
		running("a", "candidate", 7, ""),
		running("b", "candidate", 7, ""),
		running("c", "follower", 6, ""),
	})

	require.Equal(t, "", view.Leader)
	require.True(t, view.HasQuorum, "an election is not a loss of quorum")
}

// Quorum is a question about connectivity, not about how many processes
// happen to be alive.
func TestQuorumFollowsConnectivityNotNodeCount(t *testing.T) {
	c := newTestCluster("a", "b", "c", "d", "e")

	t.Run("three against two keeps a majority", func(t *testing.T) {
		view := c.summarize([]NodeView{
			running("a", "leader", 3, "a", "d", "e"),
			running("b", "follower", 3, "a", "d", "e"),
			running("c", "follower", 3, "a", "d", "e"),
			running("d", "candidate", 9, "", "a", "b", "c"),
			running("e", "candidate", 9, "", "a", "b", "c"),
		})
		require.True(t, view.HasQuorum)
		require.Equal(t, 3, view.LargestGroup)
		require.Equal(t, uint64(3), view.Term, "the cluster's term, not the cut-off minority's")
		require.Equal(t, uint64(9), view.HighestTerm)
	})

	t.Run("all five alive but split three ways has none", func(t *testing.T) {
		view := c.summarize([]NodeView{
			running("a", "candidate", 9, "", "c", "d", "e"),
			running("b", "candidate", 9, "", "c", "d", "e"),
			running("c", "candidate", 9, "", "a", "b", "e"),
			running("d", "candidate", 9, "", "a", "b", "e"),
			running("e", "candidate", 9, "", "a", "b", "c", "d"),
		})
		require.Equal(t, 5, view.Running, "every node is up")
		require.False(t, view.HasQuorum, "and none of them can commit anything")
		require.Equal(t, 3, view.Groups)
		require.Equal(t, 2, view.LargestGroup)
	})

	t.Run("crashed nodes take the quorum with them", func(t *testing.T) {
		view := c.summarize([]NodeView{
			running("a", "candidate", 4, ""),
			running("b", "candidate", 4, ""),
			{ID: "c", Lifecycle: "crashed"},
			{ID: "d", Lifecycle: "crashed"},
			{ID: "e", Lifecycle: "unreachable"},
		})
		require.False(t, view.HasQuorum)
		require.Equal(t, 2, view.Running)
	})
}

// A node cut off from the majority campaigns several times a second,
// forever. Reported one for one, those term bumps would be the only thing
// anyone ever saw in the log.
func TestTermEventsAreThrottledPerNode(t *testing.T) {
	c := newTestCluster("a", "b", "c")

	for term := uint64(2); term < 40; term++ {
		c.recordTermAdvance(running("a", "candidate", term, ""), 1)
	}

	events := c.EventsSince(0)
	require.Len(t, events, 1, "38 term bumps in a moment should collapse to one event")
	require.Contains(t, events[0].Text, "standing for election and losing")

	// A different node is throttled separately: one noisy node must not
	// silence the rest of the cluster.
	c.recordTermAdvance(running("b", "follower", 41, "c"), 30)
	require.Len(t, c.EventsSince(0), 2)
}

// Four followers adopting the winner's term is not four pieces of news; it
// is the election that was already reported, restated once per node.
func TestFollowersAdoptingTheNewTermAreNotReported(t *testing.T) {
	c := newTestCluster("a", "b", "c")

	c.recordTermAdvance(running("b", "follower", 5, "a"), 1)
	c.recordTermAdvance(running("c", "follower", 5, "a"), 1)
	require.Empty(t, c.EventsSince(0))

	// A node returning from a partition carrying a term far ahead of the
	// cluster's is a different matter, and does get reported.
	c.recordTermAdvance(running("b", "follower", 900, "a"), 895)
	require.Len(t, c.EventsSince(0), 1)
}

// An open tab costs bandwidth for as long as it is open, so a poll that
// found nothing new should not turn into a frame on the wire.
func TestOnlyMeaningfulChangesCountAsChanges(t *testing.T) {
	c := newTestCluster("a", "b", "c")

	nodes := []NodeView{
		running("a", "leader", 3, "a"),
		running("b", "follower", 3, "a"),
		running("c", "follower", 3, "a"),
	}
	nodes[0].Followers = map[string]FollowerProgress{
		"b": {NextIndex: 9, MatchIndex: 8},
		"c": {NextIndex: 9, MatchIndex: 8},
	}
	first := Snapshot{TS: 1000, Cluster: c.summarize(nodes), Nodes: nodes}

	t.Run("a later poll of an unchanged cluster", func(t *testing.T) {
		later := append([]NodeView(nil), nodes...)
		for i := range later {
			later[i].UptimeMs += 5_000 // the only thing a quiet cluster changes
		}
		second := Snapshot{TS: 6000, Cluster: c.summarize(later), Nodes: later}
		require.True(t, materiallyEqual(first, second))
	})

	t.Run("a follower falling behind", func(t *testing.T) {
		later := append([]NodeView(nil), nodes...)
		later[1].LastLogIndex = 7
		second := Snapshot{TS: 1200, Cluster: c.summarize(later), Nodes: later}
		require.False(t, materiallyEqual(first, second))
	})

	t.Run("replication progress on the leader", func(t *testing.T) {
		later := append([]NodeView(nil), nodes...)
		later[0].Followers = map[string]FollowerProgress{
			"b": {NextIndex: 10, MatchIndex: 9},
			"c": {NextIndex: 9, MatchIndex: 8},
		}
		second := Snapshot{TS: 1200, Cluster: c.summarize(later), Nodes: later}
		require.False(t, materiallyEqual(first, second))
	})

	t.Run("a node going down", func(t *testing.T) {
		later := append([]NodeView(nil), nodes...)
		later[2] = NodeView{ID: "c", Lifecycle: "crashed"}
		second := Snapshot{TS: 1200, Cluster: c.summarize(later), Nodes: later}
		require.False(t, materiallyEqual(first, second))
	})

	t.Run("a partition appearing", func(t *testing.T) {
		later := append([]NodeView(nil), nodes...)
		later[2].IsolatedFrom = []string{"a"}
		second := Snapshot{TS: 1200, Cluster: c.summarize(later), Nodes: later}
		require.False(t, materiallyEqual(first, second))
	})
}

func TestLimiterRefillsOverTime(t *testing.T) {
	l := newLimiter(60) // one per second

	for i := 0; i < 60; i++ {
		require.True(t, l.allow("1.2.3.4"), "the full budget should be available at once")
	}
	require.False(t, l.allow("1.2.3.4"), "and then be exhausted")
	require.True(t, l.allow("5.6.7.8"), "budgets are per client")

	// Rewinding the bucket's clock is how time passing is simulated
	// without the test taking that long to run.
	l.mu.Lock()
	l.buckets["1.2.3.4"].last = time.Now().Add(-5 * time.Second)
	l.mu.Unlock()

	require.True(t, l.allow("1.2.3.4"), "waiting should return part of the budget")
}
