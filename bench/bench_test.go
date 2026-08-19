package bench_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sujalbistaa/DistKV/bench"
	"github.com/sujalbistaa/DistKV/internal/kv"
	"github.com/sujalbistaa/DistKV/internal/raft"
	"github.com/sujalbistaa/DistKV/internal/storage"
	"github.com/sujalbistaa/DistKV/internal/testutil"
	"github.com/sujalbistaa/DistKV/internal/transport"
)

// benchCluster is a Raft cluster with a kv.Server per node, plus the
// leader lookup the load generator needs.
type benchCluster struct {
	*testutil.RaftCluster
	servers map[transport.NodeID]*kv.Server
}

func newBenchCluster(t *testing.T, n int, seed int64, opts ...testutil.RaftClusterOpt) *benchCluster {
	t.Helper()
	rc := testutil.NewRaftCluster(t, n, seed, opts...)
	bc := &benchCluster{RaftCluster: rc, servers: make(map[transport.NodeID]*kv.Server, n)}
	for id, node := range rc.Nodes {
		srv := kv.NewServer(node, kv.NewStore())
		bc.servers[id] = srv
		t.Cleanup(srv.Stop)
	}
	return bc
}

// onDisk gives each node a real write-ahead log, so every appended entry
// is fsynced before it is acknowledged — the cost the default in-memory
// storage doesn't pay.
func onDisk(t *testing.T) testutil.RaftClusterOpt {
	return func(i int, cfg *raft.Config) {
		ds, err := storage.NewDiskStorage(t.TempDir())
		require.NoError(t, err)
		cfg.Storage = ds
	}
}

// setLatency fixes the simulated per-message network latency, so the
// reported numbers have a stated link cost rather than an implicit one.
func (bc *benchCluster) setLatency(d time.Duration) {
	bc.Network.SetLatency(d, d)
}

// leader implements bench.LeaderFunc.
func (bc *benchCluster) leader() (transport.NodeID, bench.KV) {
	for id, node := range bc.Nodes {
		if _, role := node.State(); role == raft.Leader {
			return id, bc.servers[id]
		}
	}
	return "", nil
}

// benchDuration is short by default so this runs as part of the normal
// suite; set DISTKV_BENCH_DURATION for the longer runs used to produce
// the numbers in docs/benchmarks.md.
func benchDuration(t *testing.T) time.Duration {
	t.Helper()
	d := 3 * time.Second
	if s := os.Getenv("DISTKV_BENCH_DURATION"); s != "" {
		parsed, err := time.ParseDuration(s)
		require.NoError(t, err, "DISTKV_BENCH_DURATION")
		d = parsed
	}
	return d
}

// TestBenchmarkWorkloads measures throughput and latency percentiles for
// read-heavy, write-heavy, and mixed workloads at 3 and 5 nodes. It is a
// test rather than a Go benchmark because a fixed-duration, fixed-
// concurrency load is what the report calls for, not Go's iteration-count
// scaling.
func TestBenchmarkWorkloads(t *testing.T) {
	duration := benchDuration(t)
	workloads := []bench.Workload{bench.ReadHeavy, bench.WriteHeavy, bench.Mixed}
	nodeCounts := []int{3, 5}
	const clients = 16

	var results []bench.Result
	for _, nodes := range nodeCounts {
		for _, w := range workloads {
			bc := newBenchCluster(t, nodes, 900+int64(nodes))
			bc.setLatency(linkLatency)
			bc.AwaitLeader(t, 5*time.Second)

			result := bench.Run(context.Background(), bench.Config{
				Workload: w,
				Nodes:    nodes,
				Clients:  clients,
				Duration: duration,
				Keys:     100,
				Seed:     int64(nodes) * 31,
			}, bc.leader)

			results = append(results, result)
			t.Log(result.String())
			require.Greater(t, result.Ops, 0, "%s at %d nodes recorded no successful operations", w.Name, nodes)
		}
	}

	writeReport(t, results, duration, clients)
}

// linkLatency is the simulated one-way per-message delay applied to every
// RPC in these runs. It is stated explicitly because it dominates the
// latency numbers: a commit costs at least one leader-to-follower round
// trip, so ~2x this figure is the floor for any operation.
const linkLatency = 500 * time.Microsecond

// TestBenchmarkDiskVersusMemory quantifies what durability costs: the same
// mixed workload against in-memory storage and against a real write-ahead
// log that fsyncs every appended entry before acknowledging it.
func TestBenchmarkDiskVersusMemory(t *testing.T) {
	duration := benchDuration(t)
	const clients = 16

	run := func(name string, opts ...testutil.RaftClusterOpt) bench.Result {
		bc := newBenchCluster(t, 3, 1234, opts...)
		bc.setLatency(linkLatency)
		bc.AwaitLeader(t, 5*time.Second)
		r := bench.Run(context.Background(), bench.Config{
			Workload: bench.Workload{Name: name, ReadFraction: bench.Mixed.ReadFraction},
			Nodes:    3,
			Clients:  clients,
			Duration: duration,
			Keys:     100,
			Seed:     7,
		}, bc.leader)
		t.Log(r.String())
		require.Greater(t, r.Ops, 0)
		return r
	}

	inMemory := run("mixed/memory")
	onDiskResult := run("mixed/wal-fsync", onDisk(t))

	appendReport(t, fmt.Sprintf("\n## Durability cost: in-memory state vs. fsynced write-ahead log\n\n"+
		"3 nodes, %d clients, mixed workload, %s per run.\n\n```\n%s\n%s\n```\n\n"+
		"The fsynced run does the same consensus work plus a real `fsync` on every\n"+
		"appended entry before it is acknowledged.\n",
		clients, duration, inMemory.String(), onDiskResult.String()))
}

// TestLeaderReelectionTime measures how long a cluster takes to elect a
// new leader after the current one crashes, as a distribution over many
// trials rather than a single number.
func TestLeaderReelectionTime(t *testing.T) {
	trials := 20
	if s := os.Getenv("DISTKV_REELECTION_TRIALS"); s != "" {
		parsed, err := time.ParseDuration(s + "s")
		require.NoError(t, err)
		trials = int(parsed.Seconds())
	}

	var samples []time.Duration
	for i := 0; i < trials; i++ {
		bc := newBenchCluster(t, 5, 1000+int64(i))
		old := bc.AwaitLeader(t, 5*time.Second)

		// Let the cluster settle so we're timing a steady-state failover
		// rather than the tail of the initial election.
		time.Sleep(50 * time.Millisecond)

		crashedAt := time.Now()
		bc.Network.Crash(old.ID())

		var elected time.Duration
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			found := false
			for id, node := range bc.Nodes {
				if id == old.ID() {
					continue
				}
				if _, role := node.State(); role == raft.Leader {
					elected = time.Since(crashedAt)
					found = true
					break
				}
			}
			if found {
				break
			}
			time.Sleep(time.Millisecond)
		}
		require.NotZero(t, elected, "trial %d: no new leader was elected within 5s", i)
		samples = append(samples, elected)
	}

	dist := bench.Summarize(samples)
	t.Logf("leader re-election after crash (5 nodes, %d trials): %s", trials, dist)

	appendReport(t, fmt.Sprintf("\n## Leader re-election after leader crash\n\n"+
		"5 nodes, %d trials, election timeout 30-60ms, heartbeat 10ms, tick 3ms.\n\n"+
		"```\n%s\n```\n", trials, dist))
}

const reportPath = "results.txt"

// reporting is opt-in via DISTKV_BENCH_REPORT=1. Without it, a routine
// `go test ./...` — which runs packages in parallel, and usually under
// -race — would overwrite the recorded results with numbers measured on a
// contended machine, which are not comparable to a clean run and would
// silently invalidate docs/benchmarks.md.
func reporting() bool { return os.Getenv("DISTKV_BENCH_REPORT") == "1" }

// writeReport records the measured results next to the benchmark code, so
// docs/benchmarks.md is written from real output rather than from memory.
func writeReport(t *testing.T, results []bench.Result, duration time.Duration, clients int) {
	t.Helper()
	if !reporting() {
		return
	}
	var out string
	out += fmt.Sprintf("DistKV benchmark results — %s\n\n", time.Now().Format(time.RFC3339))
	out += fmt.Sprintf("Per-run duration %s, %d concurrent clients, 100 keys, 64-byte values,\n"+
		"simulated one-way link latency %s, in-memory storage.\n\n", duration, clients, linkLatency)
	out += "## Throughput and latency by workload\n\n```\n"
	for _, r := range results {
		out += r.String() + "\n"
	}
	out += "```\n"
	require.NoError(t, os.WriteFile(reportPath, []byte(out), 0o644))
}

func appendReport(t *testing.T, s string) {
	t.Helper()
	if !reporting() {
		return
	}
	f, err := os.OpenFile(reportPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.WriteString(s)
	require.NoError(t, err)
}
