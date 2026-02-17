# distributed-kv

A distributed in-memory key-value store built in C++20

## Features

- **Consistent hashing** with virtual nodes for even data distribution
- **Quorum-based replication** (W + R > N) for tunable consistency
- **Write-ahead logging** with CRC32 checksums and periodic snapshots
- **Reactor pattern** networking with epoll/kqueue
- **Zero heavy dependencies** — raw POSIX sockets, standard C++ threading

## Building

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

## Running

```bash
./bin/dkv_node --node-id 1 --port 7001 --cluster-conf ../cluster.conf.example
```

Run `--help` for all options.

## Testing

```bash
cd build
ctest --output-on-failure
```

## Project Structure

```
src/
├── config/        Configuration parsing
├── storage/       Storage engine, WAL, snapshots
├── network/       Event loop, TCP server, connection pool
├── cluster/       Hash ring, coordinator, membership
└── replication/   Quorum handler, read repair, hinted handoff
```

## License

MIT
