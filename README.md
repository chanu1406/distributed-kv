# distributed-kv

A distributed in-memory key-value store built in C++20 with zero heavy runtime or networking framework dependencies. Implements consistent hashing, quorum-based replication, and crash-safe durability using raw POSIX sockets and standard C++ threading.


## Architecture

```
┌──────────────────────────────────────────────────┐
│                   Client Request                 │
└──────────────┬───────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────┐
│  TCP Server (Reactor: epoll/kqueue)              │
│  Single event-loop thread + worker thread pool   │
└──────────────┬───────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────┐
│  Coordinator                                     │
│  Routes keys via consistent hash ring(MurmurHash3│
│  + virtual nodes) to the correct N replicas      │
└──────────────┬───────────────────────────────────┘
               ▼
┌───────────────────────┐  ┌───────────────────────┐
│  Quorum Handler       │  │  Replication           │
│  W + R > N consensus  │  │  Hinted handoff,       │
│  Sloppy quorum        │  │  read repair           │
└───────────┬───────────┘  └───────────────────────┘
            ▼
┌──────────────────────────────────────────────────┐
│  Storage Engine                                  │
│  32-shard concurrent map with shared_mutex       │
│  Last-Write-Wins conflict resolution             │
│  Tombstone-based deletes for consistency         │
└──────────────┬───────────────────────────────────┘
               ▼
┌───────────────────────┐  ┌───────────────────────┐
│  Write-Ahead Log      │  │  Snapshots             │
│  Binary format + CRC32│  │  Periodic serialization │
│  Batched fsync        │  │  WAL compaction        │
└───────────────────────┘  └───────────────────────┘
```

## Key Design Decisions

- **Conflict resolution**: Last-Write-Wins with `(timestamp_ms, node_id)` versioning. Deterministic, but requires reasonable NTP sync across nodes.
- **Tombstone deletes**: `DEL` writes a tombstone instead of erasing the key, preserving version info so read repair can't resurrect deleted keys.
- **Networking**: Raw POSIX sockets with a reactor pattern — no Boost, no libuv, no frameworks.
- **Durability**: WAL records are CRC32-checksummed. Recovery halts at the first corrupted record to prevent bad data from entering the store.
- **Hashing**: MurmurHash3 for deterministic key distribution (unlike `std::hash`, which varies across platforms).

## Features

- Consistent hashing with configurable virtual nodes
- Quorum-based replication (tunable W, R, N with `W + R > N` invariant)
- Write-ahead logging with CRC32 integrity and crash-safe recovery
- Periodic snapshots with WAL compaction
- Sharded storage engine with reader-writer locks for concurrent access
- Heartbeat-based failure detection with configurable timeouts
- Hinted handoff for temporary node failures
- Read repair for passive anti-entropy

## Building

Requires a C++20 compiler and CMake 3.14+. Linux or macOS (POSIX).

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

## Running

```bash
./bin/dkv_node --node-id 1 --port 7001 --cluster-conf ../cluster.conf.example
```

Run `--help` for all options including replication factors, WAL/snapshot directories, and thread pool sizing.

## Testing

```bash
cd build
ctest --output-on-failure
```

Currently **154+ tests** across 16 test files covering all components:

| Component | Tests |
|-----------|-------|
| MurmurHash3 | 7 |
| CRC32 | 5 |
| Config | 5 |
| Logger | 11 |
| Storage Engine | 11 |
| Write-Ahead Log | 16 |
| Snapshots | 3 |
| Protocol | 30+ |
| Thread Pool | 5 |
| Hash Ring | 6 |
| Cluster Config | 5 |
| Connection Pool | 6 |
| Coordinator | 14+ |
| Membership | 10 |
| Heartbeat | 6 |
| Hint Store | 13 |
| TCP Server (integration) | 6 |

## Project Structure

```
include/
├── cluster/       HashRing, Coordinator, Membership, Heartbeat, ConnectionPool, ClusterConfig
├── config/        Config struct and CLI parsing
├── network/       Poller (epoll/kqueue), TCPServer, ThreadPool, Protocol
├── replication/   HintStore
├── storage/       StorageEngine, WAL, Snapshot headers
├── utils/         MurmurHash3, CRC32, Logger
src/
├── cluster/       Hash ring, coordinator, membership, heartbeat, connection pool
├── config/        Configuration parsing implementation
├── network/       Event loop, TCP server, protocol parser, thread pool
├── replication/   Hinted handoff persistence
├── storage/       Storage engine, WAL, snapshots
├── utils/         MurmurHash3, CRC32, logger
tests/
├── unit/          Google Test suites for all components
└── integration/   TCP server integration tests
bench/
└── bench_storage.cpp  Storage engine microbenchmarks
```

## License

MIT
