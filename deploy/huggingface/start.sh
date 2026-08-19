#!/usr/bin/env bash
#
# Bring up the five nodes, then the gateway, inside one container.
#
# No process supervisor: there is nothing here for one to do. The nodes are
# never expected to exit — the console's "crash" button destroys a node
# *inside* its process and rebuilds it from disk, precisely so that a
# crashed node stays reachable enough to be restarted. A node process that
# does exit is therefore a real failure, and the right response is to let
# the container die so the platform restarts the whole thing clean.

set -euo pipefail

DATA="${DISTKV_DATA:-$HOME/data}"
PORT="${PORT:-7860}"
PEERS="node1=127.0.0.1:7071,node2=127.0.0.1:7072,node3=127.0.0.1:7073,node4=127.0.0.1:7074,node5=127.0.0.1:7075"

mkdir -p "$DATA"

pids=()
for i in 1 2 3 4 5; do
    mkdir -p "$DATA/node$i"
    distkv-server \
        -id="node$i" \
        -listen="127.0.0.1:707$i" \
        -peers="$PEERS" \
        -data-dir="$DATA/node$i" &
    pids+=("$!")
done

# Give visitors something to read rather than an empty store. Seeding goes
# through the ordinary client, which means it also waits out the first
# election for us: until a leader exists there is nobody to accept a write,
# and the retry loop is what that looks like from outside.
seed() {
    for _ in $(seq 1 40); do
        if distkv-cli -endpoints 127.0.0.1:7071 -timeout 3s put city kathmandu >/dev/null 2>&1; then
            distkv-cli -endpoints 127.0.0.1:7071 -timeout 3s put region bagmati >/dev/null 2>&1 || true
            distkv-cli -endpoints 127.0.0.1:7071 -timeout 3s put note "try crashing the leader" >/dev/null 2>&1 || true
            echo "distkv: seeded the store" >&2
            return
        fi
        sleep 1
    done
    echo "distkv: gave up seeding; the cluster is still usable" >&2
}
seed &

# -trust-proxy-headers because Spaces sit behind a proxy: without it every
# visitor shares one rate-limit budget, since every request arrives from
# the same address.
distkv-gateway \
    -listen=":$PORT" \
    -nodes="$PEERS" \
    -web=/srv/console \
    -trust-proxy-headers \
    -auto-heal=3m &
pids+=("$!")

trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT INT TERM

# Exit as soon as any of them does, rather than sitting there serving a
# console attached to a cluster that has lost a node for reasons nobody
# asked for.
#
# The pids are named explicitly rather than waiting on any job at all,
# because the seeder is a job too: it finishes a few seconds in, by design,
# and a bare `wait -n` would take that for a node dying and shut the
# container down every time it started.
wait -n "${pids[@]}"
echo "distkv: a process exited unexpectedly; stopping the container" >&2
exit 1
