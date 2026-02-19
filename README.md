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

Currently **33 tests** covering:

| Component | Tests |
|-----------|-------|
| MurmurHash3 | 6 |
| CRC32 | 5 |
| Config | 5 |
| Storage Engine | 9 |
| Write-Ahead Log | 5 |
| Snapshots | 3 |

## Project Structure

```
include/
├── config/        Config struct and CLI parsing
├── storage/       StorageEngine, WAL, Snapshot headers
├── utils/         MurmurHash3, CRC32
src/
├── config/        Configuration parsing implementation
├── storage/       Storage engine, WAL, snapshots
├── network/       Event loop, TCP server (Phase 2)
├── cluster/       Hash ring, coordinator (Phase 2)
└── replication/   Quorum handler, read repair (Phase 3)
tests/
└── unit/          Google Test suites for all components
```

## License

MIT
