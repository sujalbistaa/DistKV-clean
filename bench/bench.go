// Package bench is a load generator for DistKV: it drives a configurable
// mix of reads and writes at a cluster and reports throughput and latency
// percentiles, plus the time a cluster takes to elect a new leader after
// the current one crashes.
//
// It runs against an in-process cluster over the fake transport rather
// than over gRPC, so the numbers measure the consensus and state machine
// path — replication, fsync, commit, apply — without a network stack or
// serialization in the way. See docs/benchmarks.md for methodology.
package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/sujalbistaa/DistKV/internal/transport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

// KV is the subset of a node's API the load generator uses.
type KV interface {
	Get(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error)
	Put(ctx context.Context, cmd *pb.Command) (*pb.CommandReply, error)
}

// Workload describes a read/write mix.
type Workload struct {
	Name string
	// ReadFraction is the share of operations that are reads, in [0,1].
	ReadFraction float64
}

var (
	ReadHeavy  = Workload{Name: "read-heavy", ReadFraction: 0.95}
	WriteHeavy = Workload{Name: "write-heavy", ReadFraction: 0.05}
	Mixed      = Workload{Name: "mixed", ReadFraction: 0.50}
)

// Config parameterizes a run.
type Config struct {
	Workload    Workload
	Nodes       int
	Clients     int
	Duration    time.Duration
	Keys        int
	ValueSize   int
	OpTimeout   time.Duration
	Seed        int64
	WarmupOps   int
	MaxAttempts int
}

// Result is one run's measurements.
type Result struct {
	Workload   string
	Nodes      int
	Clients    int
	Duration   time.Duration
	Ops        int
	Errors     int
	Throughput float64 // successful operations per second
	P50        time.Duration
	P95        time.Duration
	P99        time.Duration
	Max        time.Duration
}

func (r Result) String() string {
	return fmt.Sprintf("%-12s nodes=%d clients=%2d ops=%6d errors=%4d throughput=%8.1f op/s  p50=%8s p95=%8s p99=%8s max=%8s",
		r.Workload, r.Nodes, r.Clients, r.Ops, r.Errors, r.Throughput,
		round(r.P50), round(r.P95), round(r.P99), round(r.Max))
}

func round(d time.Duration) time.Duration {
	if d >= time.Millisecond {
		return d.Round(10 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}

// LeaderFunc reports which node currently claims leadership, or "" if none
// does. The load generator uses it to route requests without having to
// know about Raft internals.
type LeaderFunc func() (transport.NodeID, KV)

// Run drives cfg.Clients goroutines against whichever node currently
// leads, for cfg.Duration, and returns the measured result. Latency is
// measured per logical operation, including any internal retries, since
// that is what a client actually experiences.
func Run(ctx context.Context, cfg Config, leader LeaderFunc) Result {
	if cfg.Keys <= 0 {
		cfg.Keys = 100
	}
	if cfg.ValueSize <= 0 {
		cfg.ValueSize = 64
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = 2 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 20
	}

	value := make([]byte, cfg.ValueSize)
	for i := range value {
		value[i] = 'x'
	}
	payload := string(value)

	runCtx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	var (
		mu      sync.Mutex
		samples []time.Duration
		errors  int
	)

	var wg sync.WaitGroup
	start := time.Now()
	for c := 0; c < cfg.Clients; c++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(clientNum) + 1))
			clientID := fmt.Sprintf("bench-%d", clientNum)
			var seq uint64
			local := make([]time.Duration, 0, 4096)
			localErrs := 0

			for runCtx.Err() == nil {
				seq++
				key := fmt.Sprintf("key-%d", rng.Intn(cfg.Keys))
				isRead := rng.Float64() < cfg.Workload.ReadFraction
				cmd := &pb.Command{ClientId: clientID, SeqNo: seq, Key: key}
				if !isRead {
					cmd.Value = payload
				}

				opStart := time.Now()
				ok := false
				for attempt := 0; attempt < cfg.MaxAttempts && runCtx.Err() == nil; attempt++ {
					_, kv := leader()
					if kv == nil {
						time.Sleep(2 * time.Millisecond)
						continue
					}
					attemptCtx, attemptCancel := context.WithTimeout(runCtx, cfg.OpTimeout)
					var reply *pb.CommandReply
					var err error
					if isRead {
						reply, err = kv.Get(attemptCtx, cmd)
					} else {
						reply, err = kv.Put(attemptCtx, cmd)
					}
					attemptCancel()
					if err == nil && reply.Success {
						ok = true
						break
					}
					time.Sleep(2 * time.Millisecond)
				}
				elapsed := time.Since(opStart)

				if ok {
					local = append(local, elapsed)
				} else if runCtx.Err() == nil {
					localErrs++
				}
			}

			mu.Lock()
			samples = append(samples, local...)
			errors += localErrs
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	result := Result{
		Workload: cfg.Workload.Name,
		Nodes:    cfg.Nodes,
		Clients:  cfg.Clients,
		Duration: elapsed,
		Ops:      len(samples),
		Errors:   errors,
	}
	if elapsed > 0 {
		result.Throughput = float64(len(samples)) / elapsed.Seconds()
	}
	if len(samples) > 0 {
		result.P50 = percentile(samples, 0.50)
		result.P95 = percentile(samples, 0.95)
		result.P99 = percentile(samples, 0.99)
		result.Max = samples[len(samples)-1]
	}
	return result
}

// percentile returns the p-th percentile of an already-sorted slice, using
// nearest-rank.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Distribution summarizes a set of measurements, used for reporting
// leader re-election times over many trials.
type Distribution struct {
	Samples []time.Duration
	Min     time.Duration
	P50     time.Duration
	P95     time.Duration
	P99     time.Duration
	Max     time.Duration
	Mean    time.Duration
}

// Summarize computes a Distribution over the given samples.
func Summarize(samples []time.Duration) Distribution {
	if len(samples) == 0 {
		return Distribution{}
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, s := range sorted {
		total += s
	}
	return Distribution{
		Samples: sorted,
		Min:     sorted[0],
		P50:     percentile(sorted, 0.50),
		P95:     percentile(sorted, 0.95),
		P99:     percentile(sorted, 0.99),
		Max:     sorted[len(sorted)-1],
		Mean:    total / time.Duration(len(sorted)),
	}
}

func (d Distribution) String() string {
	return fmt.Sprintf("n=%d min=%s p50=%s p95=%s p99=%s max=%s mean=%s",
		len(d.Samples), round(d.Min), round(d.P50), round(d.P95), round(d.P99), round(d.Max), round(d.Mean))
}
