# DistKV design notes

This document covers the decisions that aren't obvious from reading the code,
and the places where the implementation deliberately diverges from the Raft
paper or takes a simpler road than a production system would.

## How linearizable reads are implemented

**Reads are routed through the Raft log,** exactly like writes. A `Get` becomes
a log entry, is replicated, commits, and is answered only when the state machine
applies it.

The alternative, and the faster one, is *read-index*: the leader records its
current commit index, confirms it is still leader by collecting a heartbeat
quorum, waits for its state machine to catch up to that index, and then answers
from local memory without touching the log at all. That avoids a disk write and
a log entry per read.

The reason this project routes reads through the log instead:

- **Staleness becomes impossible by construction rather than by argument.** The
  danger with reads is a leader that has been deposed but doesn't know it yet.
  It still has data in memory and will happily serve it. With reads in the log,
  such a leader cannot commit the read at all — it can't reach a majority — so
  the request fails instead of returning a value that is no longer current.
  `TestReadsNeverStaleAcrossPartition` exercises precisely this: an isolated
  ex-leader is asked for a key that the majority has since overwritten, and it
  returns an error rather than the stale value.
- **Read-index has a prerequisite that is easy to miss.** A leader may only
  serve read-index reads *after committing an entry from its own current term*
  (Ongaro's dissertation, §6.4). Otherwise it may not yet know about entries
  that committed under the previous leader, and could answer from a state
  machine that is behind. Satisfying that means committing a no-op on election,
  which is another moving part with its own edge cases.

The cost is real and shows up directly in the benchmarks: read-heavy and
write-heavy workloads have effectively identical throughput and latency, because
under this design a read *is* a write as far as consensus is concerned. A
read-index implementation would make the read-heavy column several times faster.
That is the trade-off, made in favour of a correctness property that is checked
rather than argued.

## How exactly-once is achieved

Every mutating request carries a `client_id` and a `seq_no`. The state machine
keeps, per client, the sequence number and the reply of that client's most
recent mutating request. When a command arrives whose sequence number matches
what's already recorded, the cached reply is returned and the command is **not**
applied again.

Three details matter:

1. **The dedup table is part of the state machine, not the server.** It is
   mutated only inside `Apply`, so it replicates through the log and is included
   in snapshots like any other state. That is what makes dedup survive a leader
   failover: the new leader already has the old leader's dedup records, because
   they were applied deterministically on every replica.
   `TestStoreSnapshotRestoreRoundTrip` and
   `TestRetryAcrossFailoverAppliesAppendExactlyOnce` cover the two halves of
   this.
2. **Retries must reuse the sequence number.** The routing client allocates one
   sequence number per *logical* operation and reuses it across every internal
   retry it performs after a redirect or timeout. A retry that allocated a fresh
   number would be a genuinely new operation and would apply twice.
3. **Reads are not deduplicated.** A `Get` carries a sequence number, but the
   state machine never answers one from the cache — a client that reused a
   number would otherwise be handed a stale value. Reads have no side effects,
   so replaying one is harmless.

There is a known, deliberate gap: the session table grows without bound, since
nothing expires client sessions. A production system would tie sessions to
leases and evict them.

## Why commit requires an entry from the current term

`maybeAdvanceCommitLocked` only advances `commitIndex` when a majority has
replicated an entry **from the leader's own current term**. Entries from earlier
terms are committed indirectly, as a side effect of a later current-term entry
committing.

This is Figure 8 in the paper. Counting replicas of an old-term entry is not
sufficient: an entry can be present on a majority of nodes and still be
overwritten later, because a future leader elected under the "up-to-date log"
rule may not contain it. Committing on replica count alone would mean applying
an entry that subsequently disappears. Once a current-term entry commits, the
Log Matching Property makes everything before it safe too, which is why the
indirect route is sound.

A visible consequence: **a cluster that restarts with no client traffic cannot
re-derive its old commit index.** `commitIndex` is volatile by design (Figure 2),
so after a full restart every node starts at 0 and the log is intact but nothing
is known to be committed. The cluster only re-establishes that once a new leader
commits its first current-term entry. This is correct rather than a bug, and it
is why `TestCrashAndRestartPreservesCommittedEntries` issues one write after
restarting before checking that the pre-crash entries reappear. Committing a
no-op on election would paper over it — and is exactly what a read-index
implementation would require anyway.

## What the fast-backup optimisation buys

When an `AppendEntries` consistency check fails, the naive recovery is for the
leader to decrement `nextIndex` by one and try again. A follower that is *n*
entries behind, or that has *n* conflicting entries, then costs *n* round trips
to reconcile.

The reply instead carries `ConflictTerm` and `ConflictIndex`:

- If the follower's log is simply too short, `ConflictTerm` is 0 and
  `ConflictIndex` is where the follower's log ends. The leader jumps straight
  there.
- Otherwise `ConflictTerm` is the term of the follower's entry at
  `PrevLogIndex`, and `ConflictIndex` is the first index in the follower's log
  holding that term. If the leader has entries of that term it jumps just past
  its own last one; if it has none, it jumps to `ConflictIndex`, skipping the
  follower's entire conflicting term in a single step.

This turns O(*n*) round trips into roughly one per conflicting *term* rather
than per *entry*. It matters most exactly when it is most needed: a follower
returning after a long partition, with many divergent entries, which is the
case where the naive loop is slowest and the cluster is already degraded.

## Snapshots

Snapshot data is opaque to the Raft layer — a `[]byte` the application produces
and consumes. `raft.Node` signals on `SnapshotTrigger()` when enough entries
have accumulated; the application serializes its state and calls
`Node.Snapshot(index, data)`; the log is trimmed at that point. A follower too
far behind for the leader's remaining log receives `InstallSnapshot` instead of
`AppendEntries`.

The in-memory log keeps a **boundary entry at slice position 0**, holding
`{lastIncludedTerm, lastIncludedIndex}` — `{0, 0}` when no snapshot exists. This
means `PrevLogIndex`/`PrevLogTerm` arithmetic never needs a special case for an
empty log or a freshly compacted one, at the cost of every index lookup
translating from absolute index space to slice position via `entryAtLocked`.

**Simplification:** `InstallSnapshot` sends the whole snapshot in one RPC rather
than the paper's chunked `offset`/`done` scheme. For the state sizes this
project deals with that is fine; a real deployment with multi-gigabyte snapshots
would need chunking, and would also want to stream from disk rather than hold
the blob in memory.

## Persistence

Two files per node:

- **`hardstate`** — `currentTerm` and `votedFor`, rewritten in full via
  write-temp → fsync → atomic rename. Small enough that rewriting is cheaper
  than journaling, and rename atomicity means it can never be observed torn.
- **`log.wal`** — an append-only log of length-prefixed, CRC32-checksummed
  records, fsynced before any append is acknowledged.

On load, the WAL is scanned from the start and the scan stops at the first torn
header, torn payload, checksum mismatch, or out-of-sequence index — all
signatures of a crash mid-write. The file is then physically truncated to the
last valid record boundary, so recovery is self-healing rather than requiring
manual repair.

`raft.Storage` is deliberately granular (`SaveHardState` / `AppendLog` /
`TruncateLog` / `SaveSnapshot` / `Load`) rather than a single
`SaveState(everything)`. An interface that took the whole state would force
rewriting the entire log on every vote and every appended entry, which is the
opposite of what a write-ahead log is for.

**Known cost, measured:** there is no group commit. Every appended entry gets
its own `fsync`, which the benchmarks show dominates write throughput by two
orders of magnitude. Batching concurrent proposals into a single fsync is the
single highest-value optimisation left in the codebase. See
[benchmarks.md](benchmarks.md).

## Sharding

Static, range-partitioned, one Raft group per shard, no live migration — as
scoped. The shard map is a JSON config validated into a complete partition of
the keyspace: gaps, overlaps, duplicate ids, and shards that don't reach the
ends of the keyspace are all rejected at load rather than discovered at runtime.

Shards share nothing. There is no cross-shard transaction, no two-phase commit,
and no atomicity across a multi-key operation spanning shards. That is what
makes `TestShardFailureIsIsolated` hold: killing a majority of one shard leaves
the others completely unaffected, because they never depended on it.

The router caches each shard's leader, learns it from successful requests and
from `LeaderHint` on refusals, and drops it whenever a node turns out not to
lead. On a miss the client sweeps the shard's replicas in order.

**One subtlety that cost a real bug:** each attempt against a node must be
separately time-bounded. A leader partitioned away from its majority *still
believes it is the leader* — it accepts the request and then blocks forever on a
commit that can never happen. Without a per-attempt deadline, one such node
consumes the client's entire timeout budget and the client never tries anyone
else. This was found by `TestClientFollowsLeaderRedirect`, not by inspection.

## Locking discipline

Every file holding shared state documents its locking rules at the top. The
general shape in `raft.Node`: a single `mu` guards all mutable state, and **no
code path holds `mu` while sending an RPC or otherwise blocking**. RPC sends
copy what they need out of the locked section, release, send, then re-acquire to
apply the reply — and re-check that the node is still in the term and role it
was in when it started, since anything can have changed while the lock was
released. Stale replies are discarded on that check.

The apply path is a single goroutine draining a queue, woken by a coalescing
buffered channel rather than a condition variable, so shutdown is a plain
`select` on the stop channel with no risk of a missed wakeup.

## Testing approach

Everything above the transport interface is tested in-process against a fake
network that can drop, delay, duplicate, reorder, partition, and crash, all
under a seeded RNG. That is what makes failure cases like "leader is isolated
after appending but before committing" reproducible instead of occasional.

The soak test does the same thing continuously with random faults and checks the
resulting history against a key-value model with
[porcupine](https://github.com/anishathalye/porcupine). Two supporting tests,
`TestSoakDetectsANonLinearizableHistory` and its positive counterpart, verify the
*checker* rejects a history it should reject — without them, a soak that always
passes would prove nothing.
