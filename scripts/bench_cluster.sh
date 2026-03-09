#!/usr/bin/env bash
# scripts/bench_cluster.sh
# Launches a 3-node DKV cluster, runs bench_cluster, then cleans up.
#
# Usage:
#   ./scripts/bench_cluster.sh [bench_cluster flags ...]
#
# Examples:
#   ./scripts/bench_cluster.sh --ops 100000 --threads 4 --workload mixed
#   ./scripts/bench_cluster.sh --ops 50000 --pipeline 16 --workload set

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$REPO_ROOT/build/bin"
CONF="$REPO_ROOT/cluster.conf.example"

DKV_NODE="$BIN_DIR/dkv_node"
BENCH="$BIN_DIR/bench_cluster"

# ── Sanity checks ─────────────────────────────────────────────────────────────

if [[ ! -x "$DKV_NODE" ]]; then
    echo "ERROR: dkv_node not found at $DKV_NODE" >&2
    echo "       Run: cmake --build build --target dkv_node" >&2
    exit 1
fi

if [[ ! -x "$BENCH" ]]; then
    echo "ERROR: bench_cluster not found at $BENCH" >&2
    echo "       Run: cmake --build build --target bench_cluster" >&2
    exit 1
fi

# ── Data directories ──────────────────────────────────────────────────────────

DATA_DIRS=(/tmp/dkv_bench_1 /tmp/dkv_bench_2 /tmp/dkv_bench_3)
NODE_PIDS=()

cleanup() {
    echo ""
    echo "Stopping cluster nodes..."
    for pid in "${NODE_PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    # Give them a moment to flush WALs, then force-kill any stragglers.
    sleep 0.5
    for pid in "${NODE_PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    echo "Cleaning up /tmp/dkv_bench_*..."
    rm -rf /tmp/dkv_bench_{1,2,3}
}

trap cleanup EXIT INT TERM

# ── Create data dirs ──────────────────────────────────────────────────────────

for i in 1 2 3; do
    rm -rf "/tmp/dkv_bench_$i"
    mkdir -p "/tmp/dkv_bench_$i/wal" \
             "/tmp/dkv_bench_$i/snapshots" \
             "/tmp/dkv_bench_$i/hints"
done

# ── Launch 3 nodes ────────────────────────────────────────────────────────────

echo "Starting DKV cluster (3 nodes on ports 7001/7002/7003)..."

for i in 1 2 3; do
    port=$((7000 + i))
    "$DKV_NODE" \
        --node-id "$i" --port "$port" \
        --cluster-conf "$CONF" \
        --wal-dir      "/tmp/dkv_bench_$i/wal" \
        --snapshot-dir "/tmp/dkv_bench_$i/snapshots" \
        --hints-dir    "/tmp/dkv_bench_$i/hints" \
        --log-level WARN \
        --worker-threads 4 \
        >/tmp/dkv_bench_node${i}.log 2>&1 &
    NODE_PIDS+=($!)
    echo "  node$i PID ${NODE_PIDS[-1]} on port $port"
done

# ── Wait for all nodes to accept connections ──────────────────────────────────

echo "Waiting for nodes to become ready..."
MAX_WAIT=5
for i in 1 2 3; do
    port=$((7000 + i))
    deadline=$((SECONDS + MAX_WAIT))
    while true; do
        if (echo -e "PING\n" | nc -q1 127.0.0.1 "$port" 2>/dev/null | grep -q "PONG"); then
            echo "  node$i (port $port) is ready"
            break
        fi
        if [[ $SECONDS -ge $deadline ]]; then
            echo "ERROR: node$i (port $port) did not become ready within ${MAX_WAIT}s" >&2
            cat "/tmp/dkv_bench_node${i}.log" >&2
            exit 1
        fi
        sleep 0.2
    done
done

echo ""
echo "Cluster is up. Running benchmark..."
echo ""

# ── Run bench_cluster (pass all script args through) ─────────────────────────

"$BENCH" \
    --host 127.0.0.1 \
    --port 7001 \
    --ops 100000 \
    --threads 4 \
    --workload mixed \
    "$@"

# cleanup runs automatically via trap
