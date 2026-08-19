# DistKV benchmarks

All numbers below were measured on the hardware and with the methodology
described here. Nothing is estimated or extrapolated. Re-run everything with:

```
DISTKV_BENCH_REPORT=1 DISTKV_BENCH_DURATION=5s go test ./bench/ -p 1 -v -timeout 600s
```

which rewrites `bench/results.txt`, the raw output this document is written from.

Run it on an otherwise idle machine, without `-race`, and with `-p 1`. This
matters more than it sounds: an early draft of this document was written from
numbers captured while the rest of the test suite ran in parallel under the race
detector, and throughput came out **6x lower** across the board. Report writing
is gated behind `DISTKV_BENCH_REPORT=1` specifically so a routine
`go test ./...` cannot silently overwrite these results with contended ones.

## Hardware and environment

| | |
|---|---|
| CPU | Apple M3, 8 cores |
| Memory | 16 GB |
| Disk | APFS on internal NVMe |
| OS | macOS 15.6.1 |
| Go | 1.25.0, darwin/arm64 |
| Measured | 2026-08-10 |

## Methodology, and what these numbers do and don't measure

The load generator drives an **in-process cluster over the fake transport**, not
a cluster over gRPC. Every node runs in one process; RPCs are Go function calls
through `transport.Network` with a simulated delay rather than real sockets.

That choice is deliberate — it isolates the consensus and state machine path
(replication, quorum, commit, apply, dedup) from kernel networking and protobuf
serialization — but it means these are **not** end-to-end numbers for a
deployed cluster. A real deployment adds a TCP round trip, TLS if configured,
and protobuf encode/decode per RPC. Read the numbers as an upper bound on what
the consensus layer can do, not as a service-level expectation.

Parameters held constant unless stated:

- **Simulated one-way link latency: 500µs.** Applied to every RPC in both
  directions. A commit needs at least one leader→follower→leader round trip, so
  ~1ms is the hard floor for any operation, and that floor dominates the p50s
  below.
- 16 concurrent clients, 100 keys, 64-byte values, 5s per run.
- In-memory storage (no fsync) except where the durability section says
  otherwise.
- Latency is measured **per logical operation, including internal retries**,
  which is what a client actually experiences.
- Election timeout 30–60ms, heartbeat 10ms, tick 3ms — the test-tuned values,
  not the 150–300ms production defaults.

## Throughput and latency by workload

3 and 5 nodes, in-memory storage:

```
read-heavy   nodes=3 clients=16 ops=132025 errors=0  throughput=26391.3 op/s  p50=581µs p95=664µs p99=1.00ms max=13.21ms
write-heavy  nodes=3 clients=16 ops=126171 errors=0  throughput=25220.6 op/s  p50=580µs p95=683µs p99=1.28ms max=2.00s
mixed        nodes=3 clients=16 ops=128545 errors=0  throughput=25693.7 op/s  p50=585µs p95=723µs p99=1.34ms max=17.71ms
read-heavy   nodes=5 clients=16 ops=135692 errors=0  throughput=27125.4 op/s  p50=561µs p95=631µs p99=730µs  max=2.00s
write-heavy  nodes=5 clients=16 ops=139809 errors=0  throughput=27948.1 op/s  p50=559µs p95=630µs p99=711µs  max=7.41ms
mixed        nodes=5 clients=16 ops=140075 errors=0  throughput=27972.0 op/s  p50=556µs p95=624µs p99=717µs  max=8.39ms
```

### Reading these

**Read-heavy is not faster than write-heavy.** This is the single most important
thing in the table, and it is a direct, predicted consequence of a design
decision: reads are routed through the Raft log, so a `Get` costs exactly what a
`Put` costs — one round of consensus. A read-index implementation would make the
read-heavy row several times faster at the cost of a more delicate correctness
argument. The reasoning is in [design.md](design.md#how-linearizable-reads-are-implemented);
this table is what it costs.

**3 versus 5 nodes is within run-to-run noise here, and the table should not be
read as "5 nodes is faster".** Across repeated clean runs the two configurations
trade places; one earlier run had 5 nodes ~20% *slower* on read-heavy, this one
has it ~3% faster. Two things explain why the difference is so small: the leader
contacts all followers concurrently, so a larger quorum costs the slowest of
three responses instead of the slowest of two rather than costing more work
serially, and the fixed 500µs simulated link latency dominates everything else
in the p50. On a real network, with real disks and a real quorum wait, the gap
between 3 and 5 would be more visible than it is here. Drawing a scaling
conclusion from these two columns would be over-reading them.

**The `max` column is retry latency, not a stall.** Some runs show a max of
~2.0s, which is exactly the 2s per-attempt timeout in the load generator. Those
are operations where one attempt hit a node that couldn't commit and the client
retried and succeeded — `errors=0` across every run, so nothing actually failed.
The p99 is the honest tail figure here; the max is the retry path being
exercised.

## Durability: what fsync costs

The same mixed workload, 3 nodes, against in-memory state versus a real
write-ahead log that fsyncs every appended entry before acknowledging it:

```
mixed/memory     nodes=3 clients=16 ops=113313 errors=0  throughput=22650.3 op/s  p50=597µs p95=735µs    p99=1.19ms
mixed/wal-fsync  nodes=3 clients=16 ops=   873 errors=0  throughput=  172.4 op/s  p50=83ms  p95=136.36ms p99=164.26ms
```

**A 131x throughput drop.** This is the most significant performance finding in
the project, and it is not a mystery: `AppendLog` issues one `fsync` per call,
and every proposal is a separate call. There is no group commit. Each client's
write therefore serializes behind its own durable write to APFS, and macOS's
`fsync` on this hardware costs on the order of a millisecond or more.

The fix is well understood and not implemented here: **batch concurrent
proposals into a single fsync.** Real Raft implementations accumulate entries
arriving within a short window, write them together, fsync once, and then
acknowledge them all. With 16 concurrent clients, that alone should recover most
of the gap, since the whole point is that 16 pending writes cost one fsync
instead of 16.

This is called out as the highest-value remaining optimisation in
[design.md](design.md#persistence). The in-memory numbers above are the ceiling
that a batched implementation would be working toward.

## Leader re-election after a leader crash

5 nodes, 20 trials. Each trial elects a leader, lets the cluster settle for
50ms, crashes the leader at the transport layer, and measures until some other
node reports itself leader:

```
n=20  min=30.99ms  p50=39.71ms  p95=51.39ms  p99=51.39ms  max=51.39ms  mean=39.71ms
```

The floor is the election timeout: a follower must miss heartbeats for its
randomised 30–60ms timeout before standing for election, so nothing below ~31ms
is possible by construction. The p50 of ~40ms sits right where that predicts —
the middle of the 30–60ms randomisation window — and the max of ~51ms is a
follower that happened to draw a long timeout.

Every trial in this run completed in a single election round. A separate run
recorded a 136ms outlier, which is a **split vote**: two candidates timing out
close enough together to divide the votes, forcing a second round after fresh
randomised timeouts. That is expected behaviour rather than a fault, and
randomising the timeout is exactly what keeps it occasional instead of
systematic — but it means the tail of this distribution is bimodal, and a 20-run
sample will not always capture the second mode.

Scaling to the production defaults (150–300ms election timeout) would put the
typical re-election in the 150–300ms range, with split votes pushing the tail to
roughly double that.

## Linearizability soak

Not a performance number, but the correctness result these benchmarks sit
alongside:

```
DISTKV_SOAK_DURATION=10m go test -race ./internal/kv/ -run TestLinearizabilitySoak -timeout 30m
```

**576,528 operations across 40 rounds, 10 minutes under `-race`, every round
checked linearizable by porcupine**, while the harness continuously partitioned,
crashed, and restarted nodes under a seeded RNG. Throughput under `-race` with
active fault injection was roughly 960 op/s, far below the figures above — the
race detector and the constant churn are the point of that test, not speed.

A failing run prints the seed needed to reproduce it exactly.
