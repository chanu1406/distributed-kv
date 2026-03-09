# Spec: End-to-End Cluster Benchmark (`bench_cluster`)

## Goal

Build a standalone benchmark binary (`bench/bench_cluster.cpp`) that measures
**end-to-end throughput and latency across a live running cluster** — including
the full path: TCP I/O → protocol framing → routing/forwarding → quorum
replication → WAL fsync → response. This is distinct from the existing
`bench/bench_storage.cpp`, which calls the storage engine in-process with no
network or replication overhead.

---

## What to Build

### 1. `bench/bench_cluster.cpp`

A standalone C++ binary (no dependency on `dkv_core` — it is a pure TCP
client). It:

- Opens one or more persistent TCP connections to a target node
- Sends pipelined SET / GET commands using the existing wire protocol
- Measures round-trip latency per op and aggregates throughput + percentiles
- Prints a results table matching the style of `bench_storage.cpp`

#### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | Target node host |
| `--port` | `7001` | Target node port |
| `--ops` | `100000` | Total number of operations |
| `--threads` | `1` | Concurrent client threads (each with its own TCP connection) |
| `--pipeline` | `1` | Number of in-flight requests per connection before reading responses (pipelining depth) |
| `--key-size` | `16` | Key length in bytes |
| `--val-size` | `64` | Value length in bytes |
| `--workload` | `set` | `set`, `get`, `mixed` (50/50 SET/GET), or `readonly` |
| `--warmup-ops` | `1000` | Ops to run before recording latencies |

#### Output format

Exactly matches `bench_storage.cpp`'s table format (already established):

```
Benchmark                          Throughput         Mean          P50          P99        P99.9
----------------------------------------------------------------------------------------------------
SET (1 thread, pipeline=1)        42.31 Kops/s      23.64 µs     21.10 µs     58.30 µs    102.40 µs
GET (1 thread, pipeline=1)        51.88 Kops/s      19.27 µs     17.80 µs     44.10 µs     89.20 µs
MIXED 50/50 (4 threads)           38.74 Kops/s      25.91 µs     23.50 µs     61.40 µs    115.00 µs
```

---

## Wire Protocol Reference

The protocol is implemented in `include/network/protocol.h` /
`src/network/protocol.cpp`. The benchmark must implement a **minimal subset**
of the client side of this protocol from scratch (no `dkv_core` linkage
needed — just raw POSIX sockets + `sprintf`/`send`/`recv`).

### Request format (newline-terminated)

```
SET <key_len> <key> <val_len> <value>\n
GET <key_len> <key>\n
PING\n
```

Examples (key = `"key:0000000001"`, val = 64-byte string of `'v'`):
```
SET 14 key:0000000001 64 vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv\n
GET 14 key:0000000001\n
```

### Response format

```
+OK\n                        → successful SET/DEL
$<val_len> <value>\n         → GET hit
-NOT_FOUND\n                 → GET miss
-ERR <message>\n             → error (e.g., QUORUM_FAILED, NODE_UNAVAILABLE)
+PONG\n                      → PING response
```

The benchmark client must parse these responses correctly. `-ERR` and
`-NOT_FOUND` responses during warm-up or `get`/`mixed` workloads before keys
are populated should be silently ignored; during the actual measurement phase
they should be counted as errors and printed as a summary line at the end
(e.g., `Errors: 42 / 100000 ops`).

---

## Implementation Details

### TCP connection management

- Use raw POSIX sockets: `socket()`, `connect()`, `send()`, `recv()`
- One blocking TCP socket per benchmark thread
- Apply `SO_RCVTIMEO` / `SO_SNDTIMEO` of 2000ms on each socket
- Use `TCP_NODELAY` to disable Nagle's algorithm (eliminates artificial latency
  on small frames)

### Latency measurement

- Record per-op round-trip time using `std::chrono::high_resolution_clock`
- For pipeline depth > 1: measure latency as time from sending the first
  request in a pipeline batch to receiving the last response in that batch
  (batch-level granularity is fine)
- Store raw nanosecond values; compute percentiles by sorting at the end
  (same approach as `bench_storage.cpp`'s `Stats` struct — copy that pattern)

### Pipelining

When `--pipeline N` (N > 1):
- Send N requests back-to-back without waiting for responses
- Then read all N responses
- This saturates the server's pipeline and shows peak throughput

### Workload modes

- **`set`**: All SETs, key index cycles 0..ops-1
- **`get`**: Pre-populate keys with a warm-up SET phase (not timed), then
  issue GETs for those keys
- **`mixed`**: Alternate SET and GET. Even ops → SET key `i`, odd ops → GET
  key `i-1`
- **`readonly`**: Same as `get` — alias for clarity in scripts

### Multi-threaded mode

When `--threads N`:
- Each thread opens its own TCP connection
- Each thread gets a disjoint key range: thread `t` uses keys
  `[t * (ops/N), (t+1) * (ops/N))`
- All threads start simultaneously via a `std::barrier` (C++20) or a
  `std::atomic<bool>` go-flag
- Aggregate all latency samples before computing percentiles; record wall-clock
  elapsed from first thread start to last thread finish for throughput

---

## CMakeLists.txt changes

Add to the **Benchmarks** section of the root `CMakeLists.txt`:

```cmake
add_executable(bench_cluster
    bench/bench_cluster.cpp
)
# bench_cluster is a pure POSIX TCP client — no dkv_core needed.
# Only needs pthread for std::thread.
target_link_libraries(bench_cluster PRIVATE pthread)
target_include_directories(bench_cluster PRIVATE ${CMAKE_SOURCE_DIR}/include)
```

Note: `target_include_directories` is included only because the benchmark
may optionally include the protocol header for constants/reference. If the
implementation is fully self-contained (copy-pasted format strings), it can be
omitted.

---

## `scripts/bench_cluster.sh`

A bash script that:

1. Launches 3 `dkv_node` instances (nodes 1/2/3 on ports 7001/7002/7003) from
   `build/bin/dkv_node` with the `cluster.conf.example` cluster topology,
   writing WAL/snapshot/hints data to `/tmp/dkv_bench_{1,2,3}/`
2. Waits for all 3 nodes to be ready (retry `PING` via `nc` or a small
   connect loop, up to 5 seconds)
3. Runs `build/bin/bench_cluster` with configurable flags (defaults: 100k ops,
   4 threads, pipeline=1, mixed workload, targeting port 7001)
4. On exit (including Ctrl-C via `trap`), kills all 3 background node processes
   and cleans up `/tmp/dkv_bench_*/`

Node launch command example:
```bash
./build/bin/dkv_node \
  --node-id 1 --port 7001 \
  --cluster-conf cluster.conf.example \
  --wal-dir /tmp/dkv_bench_1/wal \
  --snapshot-dir /tmp/dkv_bench_1/snapshots \
  --hints-dir /tmp/dkv_bench_1/hints \
  --log-level WARN \
  --worker-threads 4 &
```

(`--log-level WARN` silences INFO chatter during benchmarking.)

---

## Files to Create / Modify

| Action | Path |
|--------|------|
| **Create** | `bench/bench_cluster.cpp` |
| **Create** | `scripts/bench_cluster.sh` (chmod +x) |
| **Modify** | `CMakeLists.txt` — add `bench_cluster` target in Benchmarks section |

Do **not** modify any existing source files under `src/` or `include/`. The
benchmark is entirely a client-side tool.

---

## Existing Patterns to Follow

- Match the `Stats` struct / `print_header()` / `print_result()` / `fmt_ops()`
  / `fmt_us()` style from `bench/bench_storage.cpp` exactly — keep the output
  visually consistent.
- The `BenchConfig` / `BenchResult` structs and the `--ops` / `--threads`
  / `--key-size` / `--val-size` CLI parsing pattern from `bench_storage.cpp`
  should be extended (not duplicated from scratch) — copy and adapt.
- All code must compile cleanly under `-Wall -Wextra -Wpedantic -Werror`
  (enforced by `CMakeLists.txt`).
- C++20 standard is required (set project-wide in `CMakeLists.txt`).
- POSIX only (Linux / macOS). No Windows-specific APIs.
