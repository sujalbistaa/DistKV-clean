---
title: DistKV Cluster Console
emoji: 🗄️
colorFrom: gray
colorTo: indigo
sdk: docker
app_port: 7860
pinned: false
license: mit
short_description: A live 5-node Raft cluster you can crash and partition
---

# DistKV — cluster console

A distributed, fault-tolerant key-value store built on a Raft implementation
written from scratch in Go. **No `hashicorp/raft`, no `etcd-io/raft`, no
consensus library of any kind.**

This Space runs a real five-node cluster: five separate processes, each with
its own write-ahead log and snapshots, electing a leader among themselves and
replicating an append-only log over TCP. The page in front of them is attached
to that cluster — every number on it was read off a node a fraction of a second
earlier, and every button does what it says.

## Things worth doing

**Crash the leader.** The node is genuinely destroyed: goroutines stopped, file
handles closed, disk untouched. The remaining four notice within a few hundred
milliseconds and elect a replacement. Read a key afterwards — it is still there,
served by a different node than the one that took it.

**Cut the network three against two.** The majority side keeps serving. The
minority side stands for election, fails, stands again, and drives its term far
above the cluster's while winning nothing. Watching those two numbers diverge is
watching why Raft requires a majority.

**Crash three of the five.** The store stops accepting writes and says why.
This is the important one: refusing is correct, and a store that answered here
would look healthier while being wrong.

Every key-value operation shows its route — which node it hit, who refused it,
who redirected it where, and the log index it committed at.

## The receipt

A fault-injection soak test ran **576,528 operations** against this code under
`-race` while a chaos loop continuously partitioned, crashed, and restarted
nodes. Every history was checked against a key-value model with
[porcupine](https://github.com/anishathalye/porcupine): zero linearizability
violations.

## Notes

Storage here is ephemeral — a Space restart brings the cluster back empty, which
is also how it gets cleaned up. Writes are rate limited per visitor and capped
in size and number. The cluster restores itself if it is left broken and idle.

Source, design notes, and benchmarks: **https://github.com/sujalbistaa/DistKV**
