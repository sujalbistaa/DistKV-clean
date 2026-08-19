package transport_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/internal/testutil"
	"github.com/sujalbistaa/DistKV/internal/transport"
)

func echoHandler(counter *atomic.Int64) transport.Handler {
	return func(ctx context.Context, from transport.NodeID, req any) (any, error) {
		if counter != nil {
			counter.Add(1)
		}
		return req, nil
	}
}

func registerEcho(t *testing.T, c *testutil.Cluster) {
	t.Helper()
	for _, id := range c.IDs {
		c.Trans[id].RegisterHandler(echoHandler(nil))
	}
}

// TestPartitionAndHeal is the milestone 1 acceptance test: a 5-node cluster
// is partitioned 3/2, message delivery is confirmed to respect the split,
// and after healing every node can reach every other node again.
func TestPartitionAndHeal(t *testing.T) {
	c := testutil.NewCluster(t, 5, 42)
	registerEcho(t, c)
	ctx := context.Background()

	// Fully connected: every pair can reach each other.
	for _, from := range c.IDs {
		for _, to := range c.IDs {
			_, err := c.Send(ctx, from, to, "ping")
			require.NoError(t, err, "%s -> %s before partition", from, to)
		}
	}

	groupA, groupB := c.Split(3)
	c.Partition(groupA, groupB)

	within := func(group []transport.NodeID) {
		for _, from := range group {
			for _, to := range group {
				_, err := c.Send(ctx, from, to, "ping")
				require.NoError(t, err, "%s -> %s within partition", from, to)
			}
		}
	}
	within(groupA)
	within(groupB)

	for _, from := range groupA {
		for _, to := range groupB {
			_, err := c.Send(ctx, from, to, "ping")
			require.ErrorIs(t, err, transport.ErrUnreachable, "%s -> %s across partition", from, to)
			_, err = c.Send(ctx, to, from, "ping")
			require.ErrorIs(t, err, transport.ErrUnreachable, "%s -> %s across partition", to, from)
		}
	}

	c.Heal()
	for _, from := range c.IDs {
		for _, to := range c.IDs {
			_, err := c.Send(ctx, from, to, "ping")
			require.NoError(t, err, "%s -> %s after heal", from, to)
		}
	}
}

func TestDisconnectReconnect(t *testing.T) {
	c := testutil.NewCluster(t, 3, 1)
	registerEcho(t, c)
	ctx := context.Background()
	a, b := c.IDs[0], c.IDs[1]

	c.Network.Disconnect(a)
	_, err := c.Send(ctx, a, b, "x")
	require.ErrorIs(t, err, transport.ErrUnreachable)
	_, err = c.Send(ctx, b, a, "x")
	require.ErrorIs(t, err, transport.ErrUnreachable)

	c.Network.Reconnect(a)
	_, err = c.Send(ctx, a, b, "x")
	require.NoError(t, err)
}

func TestCrashRestart(t *testing.T) {
	c := testutil.NewCluster(t, 3, 2)
	registerEcho(t, c)
	ctx := context.Background()
	a, b := c.IDs[0], c.IDs[1]

	c.Network.Crash(a)
	_, err := c.Send(ctx, a, b, "x")
	require.ErrorIs(t, err, transport.ErrCrashed)
	_, err = c.Send(ctx, b, a, "x")
	require.ErrorIs(t, err, transport.ErrCrashed)

	c.Network.Restart(a)
	_, err = c.Send(ctx, a, b, "x")
	require.NoError(t, err)
}

func TestDropRate(t *testing.T) {
	c := testutil.NewCluster(t, 2, 3)
	registerEcho(t, c)
	ctx := context.Background()
	a, b := c.IDs[0], c.IDs[1]

	c.Network.SetDropRate(1.0)
	_, err := c.Send(ctx, a, b, "x")
	require.ErrorIs(t, err, transport.ErrDropped)

	c.Network.SetDropRate(0.0)
	_, err = c.Send(ctx, a, b, "x")
	require.NoError(t, err)
}

func TestDuplicateRate(t *testing.T) {
	c := testutil.NewCluster(t, 2, 4)
	var count atomic.Int64
	c.Trans[c.IDs[1]].RegisterHandler(echoHandler(&count))
	ctx := context.Background()
	a, b := c.IDs[0], c.IDs[1]

	c.Network.SetDuplicateRate(1.0)
	c.Network.SetLatency(time.Millisecond, 5*time.Millisecond)
	_, err := c.Send(ctx, a, b, "x")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return count.Load() >= 2
	}, time.Second, time.Millisecond, "handler should have run at least twice")
}

func TestNoHandlerRegistered(t *testing.T) {
	c := testutil.NewCluster(t, 2, 5)
	ctx := context.Background()
	_, err := c.Send(ctx, c.IDs[0], c.IDs[1], "x")
	require.ErrorIs(t, err, transport.ErrNoHandler)
}

func TestSendRespectsContextCancellation(t *testing.T) {
	c := testutil.NewCluster(t, 2, 6)
	registerEcho(t, c)
	c.Network.SetLatency(time.Hour, time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := c.Send(ctx, c.IDs[0], c.IDs[1], "x")
	require.True(t, errors.Is(err, context.DeadlineExceeded))
}

// TestDeterministicSeed asserts that two networks built from the same seed
// make identical drop decisions across the same sequence of Sends, so a
// failing soak test reproduces exactly.
func TestDeterministicSeed(t *testing.T) {
	const seed = 12345
	run := func() []bool {
		c := testutil.NewCluster(t, 2, seed)
		registerEcho(t, c)
		c.Network.SetDropRate(0.5)
		ctx := context.Background()
		var results []bool
		for i := 0; i < 50; i++ {
			_, err := c.Send(ctx, c.IDs[0], c.IDs[1], i)
			results = append(results, err == nil)
		}
		return results
	}

	first := run()
	second := run()
	require.Equal(t, first, second)
}
