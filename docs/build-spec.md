# DistKV — Build Spec

A fault-tolerant, sharded, distributed key-value store in Go with a hand-written Raft
implementation. This file is the contract. Build it milestone by milestone. Do not skip
ahead, and do not start a milestone until the previous one's acceptance criteria pass.

## Hard constraints

- Go 1.22+. Module path `github.com/sujalbistaa/DistKV`.
- **Raft must be implemented from scratch.** Do not import `hashicorp/raft`,
  `etcd-io/raft`, `dragonboat`, or any other consensus library. Reading the extended
  Raft paper (Ongaro & Ousterhout, 2014) is expected; copying an existing implementation
  is not.
- Allowed third-party dependencies, and nothing else without asking first:
  `google.golang.org/grpc`, `google.golang.org/protobuf`,
  `github.com/anishathalye/porcupine` (linearizability checking), `github.com/stretchr/testify`.
- Every milestone must pass `go test -race ./...` cleanly. Data races are failures, not warnings.
- No `time.Sleep` in production code paths for synchronisation. Use channels, condition
  variables, and timers. Sleeps are allowed in tests.
- All shared state is guarded by an explicit mutex. Document the locking discipline in a
  comment at the top of each file that holds state.
- One commit per milestone minimum, with a message describing what now works and what is
  tested. The commit history is part of the deliverable.

## Repository layout

```
cmd/distkv-server/      # node binary: flags for node id, peers, shard config, data dir
cmd/distkv-cli/         # client: get/put/append/delete, --endpoints
internal/raft/          # consensus: state machine, RPCs, log, persistence, snapshots
internal/storage/       # write-ahead log + on-disk state, keyspace engine
internal/kv/            # replicated KV state machine applied from the Raft log
internal/shard/         # shard map, key routing, per-shard Raft groups
internal/transport/     # RPC abstraction: gRPC in prod, in-memory fake in tests
internal/testutil/      # cluster harness, fault injection, linearizability checking
proto/                  # .proto definitions
bench/                  # load generator and benchmark reporting
docs/                   # architecture notes, design decisions, benchmark results
```

The `transport` abstraction matters: every RPC goes through an interface so tests can
drop, delay, duplicate and reorder messages without touching the network.

---

## Milestone 1 — Transport and cluster harness

Build the in-memory transport and the test harness first, before any consensus logic.
Everything downstream depends on being able to break the network deterministically.

Requirements:
- `transport.Transport` interface with `Send(ctx, to NodeID, req any) (any, error)`.
- In-memory implementation with knobs: `Disconnect(node)`, `Reconnect(node)`,
  `Partition(groupA, groupB []NodeID)`, `SetLatency(min, max)`, `SetDropRate(p)`,
  `SetDuplicateRate(p)`, `Crash(node)`, `Restart(node)`.
- Deterministic: seedable RNG so a failing test reproduces exactly.
- `testutil.Cluster` that spins up N in-process nodes sharing one fake transport.

Acceptance: a test that starts a 5-node cluster, partitions it 3/2, heals it, and asserts
message delivery follows the configured rules. `go test -race ./internal/transport/...` passes.

## Milestone 2 — Raft leader election

Requirements:
- Node states Follower, Candidate, Leader with correct transitions.
- Persistent state `currentTerm`, `votedFor`; randomised election timeouts (150–300ms base,
  tunable for tests); heartbeats via empty AppendEntries.
- `RequestVote` and `AppendEntries` RPC handlers implementing the paper's rules exactly,
  including term comparison and the "reject if candidate's log is not up to date" check.

Acceptance tests:
- Exactly one leader is elected in a 5-node cluster within one election timeout.
- Killing the leader produces exactly one new leader.
- A minority partition (2 of 5) elects no leader; the majority side does.
- The old leader steps down on rejoining and adopts the higher term.
- No test ever observes two leaders in the same term.

## Milestone 3 — Log replication

Requirements:
- Log entries `{Term, Index, Command}`; leader `nextIndex`/`matchIndex` tracking.
- Consistency check on `prevLogIndex`/`prevLogTerm`, with conflict backtracking. Implement
  the optimised fast-backup (conflict term and first index of that term), not one-index-at-a-time.
- `commitIndex` advanced only when a majority has replicated an entry **from the current term**.
- Committed entries delivered in order on an apply channel; no gaps, no duplicates, no reordering.

Acceptance tests:
- Agreement on a value with all nodes up.
- Agreement with one follower disconnected; the follower catches up after reconnect.
- No agreement with a minority; the client-visible operation does not commit.
- Concurrent proposals from many goroutines all appear exactly once in the applied log.
- Conflicting entries on a rejoining node are truncated and overwritten correctly.

## Milestone 4 — Persistence and crash recovery

Requirements:
- Write-ahead log on disk. Append-only, checksummed records, fsync before an RPC reply
  acknowledges the entry. Corrupt or torn trailing records are detected and truncated on load.
- Persist `currentTerm`, `votedFor`, and log entries; restore full state on restart.

Acceptance tests:
- Crash and restart every node; the cluster resumes and preserves all committed entries.
- Crash mid-write with a truncated final record; the node recovers to the last valid entry.
- A node that voted in term T and restarts does not vote again in term T.

## Milestone 5 — Snapshots and log compaction

Requirements:
- State machine snapshots with `lastIncludedIndex`/`lastIncludedTerm`; log trimmed at the
  snapshot point; `InstallSnapshot` RPC for followers too far behind.
- Snapshot triggered by log size threshold, configurable.

Acceptance tests:
- A follower disconnected past the compaction point recovers via InstallSnapshot.
- Log size stays bounded under sustained writes.
- Restart from a snapshot plus the tail of the log yields identical state on every node.

## Milestone 6 — Replicated KV state machine

Requirements:
- Operations Get, Put, Append, Delete, applied deterministically from the Raft log.
- **Linearizable reads.** Do not read from a stale leader. Implement either read-index with
  a heartbeat quorum confirmation, or route reads through the log. Document the choice and
  the trade-off in `docs/`.
- **Exactly-once semantics.** Client sessions with a client id and monotonic sequence number;
  the state machine deduplicates retried commands and returns the cached reply. A retried
  Append must not apply twice.
- gRPC service exposing the client API; leader redirection so a client hitting a follower
  is told where the leader is.

Acceptance tests:
- Client retries across a leader failover produce exactly one applied Append.
- Reads never return a value older than a completed prior write, including during partitions.

## Milestone 7 — Sharding

Keep this simple. Static shard assignment, one Raft group per shard, no live migration.

Requirements:
- Range-partitioned keyspace with a shard map loaded from config.
- A router that resolves a key to a shard and that shard's current leader, with caching and
  invalidation on redirect.
- Each shard group is independent: losing a majority in one shard must not affect the others.

Acceptance test: kill a majority of shard 2 and confirm shards 1 and 3 continue serving.

## Milestone 8 — Linearizability verification and benchmarks

This is the milestone that makes the project worth listing. Do not cut it.

Requirements:
- A fault-injection soak test: concurrent clients issuing random operations while the harness
  randomly partitions, crashes and restarts nodes with a seeded RNG.
- Record a history of operation invocations and responses and check it with `porcupine`
  against a KV model. A non-linearizable history fails the test and dumps the seed.
- `bench/` load generator reporting throughput and p50/p95/p99 latency for read-heavy,
  write-heavy and mixed workloads, at 3 and 5 nodes.
- Measured leader re-election time after a leader crash, as a distribution over many runs.

Acceptance: the soak test runs 10 minutes under `-race` without a linearizability violation,
and `docs/benchmarks.md` contains real measured numbers with the hardware and methodology.

---

## Documentation deliverables

- `README.md`: what it is, architecture diagram, how to run a 5-node cluster locally with
  Docker Compose, how to run the tests, and the headline benchmark numbers.
- `docs/design.md`: the non-obvious decisions and why. At minimum: how linearizable reads
  are implemented, how exactly-once is achieved, why commit requires a current-term entry,
  and what the fast-backup optimisation buys.
- `docs/benchmarks.md`: methodology and results.

## Working style

At the end of each milestone, report: what was implemented, which acceptance tests pass,
what is deliberately not handled yet, and any place where the implementation diverges from
the Raft paper and why. If a design decision has a real trade-off, surface it rather than
picking silently.