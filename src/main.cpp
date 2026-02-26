#include "cluster/cluster_config.h"
#include "cluster/connection_pool.h"
#include "cluster/coordinator.h"
#include "cluster/hash_ring.h"
#include "config/config.h"
#include "network/tcp_server.h"
#include "storage/snapshot.h"
#include "storage/storage_engine.h"
#include "storage/wal.h"

#include <csignal>
#include <iostream>

static dkv::TCPServer* g_server = nullptr;

void signal_handler(int) {
    if (g_server) g_server->stop();
}

int main(int argc, char* argv[]) {
    dkv::Config cfg = dkv::parse_args(argc, argv);
    dkv::print_config(cfg);

    // validate quorum invariant: W + R > N
    if (cfg.write_quorum + cfg.read_quorum <= cfg.replication_factor) {
        std::cerr << "[ERROR] Quorum invariant violated: W(" << cfg.write_quorum
                  << ") + R(" << cfg.read_quorum
                  << ") must be > N(" << cfg.replication_factor << ")\n";
        return 1;
    }

    // ── Parse cluster configuration ─────────────────────────────────────────
    auto cluster_entries = dkv::parse_cluster_config(cfg.cluster_conf);
    std::cout << "[BOOT] Loaded " << cluster_entries.size()
              << " nodes from " << cfg.cluster_conf << "\n";

    // ── Build hash ring ─────────────────────────────────────────────────────
    dkv::HashRing ring;
    for (const auto& entry : cluster_entries) {
        // Derive node_id from the name (e.g. "node1" -> 1, "node2" -> 2)
        uint32_t id = 0;
        for (char c : entry.name) {
            if (c >= '0' && c <= '9') {
                id = id * 10 + static_cast<uint32_t>(c - '0');
            }
        }
        if (id == 0) {
            id = static_cast<uint32_t>(std::hash<std::string>{}(entry.name) & 0xFFFFFFFF);
        }

        std::string address = entry.host + ":" + std::to_string(entry.port);
        ring.add_node(id, address, cfg.vnodes);
        std::cout << "[BOOT] Ring: " << entry.name << " (id=" << id
                  << ") -> " << address << "\n";
    }

    std::cout << "[BOOT] Hash ring: " << ring.node_count() << " physical nodes, "
              << ring.size() << " virtual nodes\n";

    // ── Boot the node ───────────────────────────────────────────────────────
    std::cout << "[BOOT] Node " << cfg.node_id
              << " listening on port " << cfg.port << "\n"
              << "[BOOT] Quorum: W=" << cfg.write_quorum
              << " R=" << cfg.read_quorum
              << " N=" << cfg.replication_factor << "\n";

    // ── Initialize storage engine ───────────────────────────────────────────
    dkv::StorageEngine engine;

    // ── Open WAL and recover from disk ──────────────────────────────────────
    dkv::WAL wal;
    if (!wal.open(cfg.wal_dir, cfg.fsync_interval_ms, /*batch_ops=*/100)) {
        std::cerr << "[FATAL] Could not open WAL at " << cfg.wal_dir << "\n";
        return 1;
    }

    // Load latest snapshot (if any) to fast-forward engine state
    uint64_t snapshot_seq = 0;
    auto snap_path = dkv::Snapshot::find_latest(cfg.snapshot_dir);
    if (snap_path.has_value()) {
        auto snap_data = dkv::Snapshot::load(*snap_path);
        if (snap_data.has_value()) {
            snapshot_seq = snap_data->seq_no;
            for (const auto& [key, entry] : snap_data->entries) {
                if (entry.is_tombstone) {
                    engine.del(key, entry.version);
                } else {
                    engine.set(key, entry.value, entry.version);
                }
            }
            std::cout << "[BOOT] Loaded snapshot at seq " << snapshot_seq
                      << " (" << snap_data->entries.size() << " entries)\n";
        }
    }

    // Replay WAL records written after the snapshot
    auto records = wal.recover();
    size_t replayed = 0;
    for (const auto& rec : records) {
        if (rec.seq_no <= snapshot_seq) continue;  // already in snapshot

        dkv::Version v{rec.timestamp_ms, cfg.node_id};
        if (rec.op_type == dkv::OpType::SET) {
            engine.set(rec.key, rec.value, v);
        } else {
            engine.del(rec.key, v);
        }
        ++replayed;
    }
    std::cout << "[BOOT] WAL: " << records.size() << " total records, "
              << replayed << " replayed after snapshot\n";

    // ── Build coordinator with durability and quorum parameters ─────────────
    dkv::ConnectionPool conn_pool;
    dkv::Coordinator coordinator(engine, ring, conn_pool, cfg.node_id,
                                 &wal, cfg.snapshot_dir, cfg.snapshot_interval,
                                 cfg.replication_factor,
                                 cfg.write_quorum,
                                 cfg.read_quorum);

    // Create TCP server in cluster mode (routes through coordinator)
    dkv::TCPServer server(engine, coordinator, cfg.port,
                          cfg.worker_threads, cfg.node_id);
    g_server = &server;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "[BOOT] Server running in cluster mode\n";
    server.run();

    // ── Graceful shutdown: flush WAL ─────────────────────────────────────────
    wal.sync();
    wal.close();
    std::cout << "[SHUTDOWN] WAL flushed and closed\n";

    return 0;
}
