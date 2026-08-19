package raft_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/testutil"
	"github.com/sujalbistaa/DistKV/internal/transport"
)

// TestElectsExactlyOneLeader: a 5-node cluster elects exactly one leader
// within one election timeout.
func TestElectsExactlyOneLeader(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 100)
	stop := monitorAtMostOneLeaderPerTerm(t, rc)
	defer stop()

	leader := rc.AwaitLeader(t, time.Second)
	require.NotNil(t, leader)
}

// TestKillingLeaderElectsNewLeader: killing the leader produces exactly one
// new leader, at a higher term.
func TestKillingLeaderElectsNewLeader(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 101)
	stop := monitorAtMostOneLeaderPerTerm(t, rc)
	defer stop()

	first := rc.AwaitLeader(t, time.Second)
	firstTerm, _ := first.State()

	rc.Network.Crash(first.ID())

	var second *raft.Node
	deadline := time.Now().Add(time.Second)
	for second == nil && time.Now().Before(deadline) {
		for _, node := range rc.Nodes {
			if node.ID() == first.ID() {
				continue
			}
			if term, role := node.State(); role == raft.Leader && term > firstTerm {
				second = node
				break
			}
		}
		if second == nil {
			time.Sleep(2 * time.Millisecond)
		}
	}
	require.NotNil(t, second, "no new leader elected after leader crash")
	require.NotEqual(t, first.ID(), second.ID())
}

// TestMinorityPartitionElectsNoLeader: a minority partition (2 of 5) elects
// no leader of its own; the majority side elects (or keeps) one.
func TestMinorityPartitionElectsNoLeader(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 102)
	stop := monitorAtMostOneLeaderPerTerm(t, rc)
	defer stop()

	rc.AwaitLeader(t, time.Second)

	majority, minority := rc.IDs[:3], rc.IDs[3:]
	rc.Partition(majority, minority)

	var majorityLeader *raft.Node
	deadline := time.Now().Add(time.Second)
	for majorityLeader == nil && time.Now().Before(deadline) {
		for _, id := range majority {
			if _, role := rc.Nodes[id].State(); role == raft.Leader {
				majorityLeader = rc.Nodes[id]
				break
			}
		}
		if majorityLeader == nil {
			time.Sleep(2 * time.Millisecond)
		}
	}
	require.NotNil(t, majorityLeader, "majority side failed to elect a leader")

	for i := 0; i < 100; i++ {
		for _, id := range minority {
			_, role := rc.Nodes[id].State()
			require.NotEqual(t, raft.Leader, role, "minority node %s became leader", id)
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// TestOldLeaderStepsDownOnRejoin: a leader isolated from the majority keeps
// believing it leads; once the majority elects a new leader at a higher
// term and the partition heals, the old leader steps down and adopts the
// higher term.
func TestOldLeaderStepsDownOnRejoin(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 103)
	stop := monitorAtMostOneLeaderPerTerm(t, rc)
	defer stop()

	oldLeader := rc.AwaitLeader(t, time.Second)
	oldTerm, _ := oldLeader.State()

	var rest []transport.NodeID
	for _, id := range rc.IDs {
		if id != oldLeader.ID() {
			rest = append(rest, id)
		}
	}
	rc.Partition([]transport.NodeID{oldLeader.ID()}, rest)

	var newLeader *raft.Node
	deadline := time.Now().Add(time.Second)
	for newLeader == nil && time.Now().Before(deadline) {
		for _, id := range rest {
			if term, role := rc.Nodes[id].State(); role == raft.Leader && term > oldTerm {
				newLeader = rc.Nodes[id]
				break
			}
		}
		if newLeader == nil {
			time.Sleep(2 * time.Millisecond)
		}
	}
	require.NotNil(t, newLeader, "majority side failed to elect a new leader")
	newTerm, _ := newLeader.State()
	require.Greater(t, newTerm, oldTerm)

	// The old leader is still isolated and has no way to learn about the
	// new term yet; it should still (wrongly) believe it leads.
	term, role := oldLeader.State()
	require.Equal(t, raft.Leader, role)
	require.Equal(t, oldTerm, term)

	rc.Heal()

	require.Eventually(t, func() bool {
		term, role := oldLeader.State()
		return role == raft.Follower && term >= newTerm
	}, time.Second, 2*time.Millisecond, "old leader should step down and adopt the higher term")
}

// TestChurnNeverProducesTwoLeadersInATerm repeatedly crashes and restarts
// the current leader while continuously checking that no two nodes ever
// report Leader for the same term.
func TestChurnNeverProducesTwoLeadersInATerm(t *testing.T) {
	rc := testutil.NewRaftCluster(t, 5, 104)
	stop := monitorAtMostOneLeaderPerTerm(t, rc)
	defer stop()

	for round := 0; round < 5; round++ {
		leader := rc.AwaitLeader(t, time.Second)
		rc.Network.Crash(leader.ID())
		time.Sleep(80 * time.Millisecond)
		rc.Network.Restart(leader.ID())
		time.Sleep(20 * time.Millisecond)
	}
}

// monitorAtMostOneLeaderPerTerm polls every node's (term, role) at a fine
// grain and fails the test immediately (via t.Errorf, safe from any
// goroutine) if two nodes ever report Leader for the same term at the same
// polling instant. The returned stop function must be called, and its
// completion awaited, before the test returns.
func monitorAtMostOneLeaderPerTerm(t *testing.T, rc *testutil.RaftCluster) (stop func()) {
	t.Helper()
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				leadersByTerm := map[uint64]int{}
				for _, node := range rc.Nodes {
					term, role := node.State()
					if role == raft.Leader {
						leadersByTerm[term]++
					}
				}
				for term, count := range leadersByTerm {
					if count > 1 {
						t.Errorf("observed %d leaders in term %d", count, term)
					}
				}
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}
