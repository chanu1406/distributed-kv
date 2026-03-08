#include "cluster/cluster_config.h"
#include "cluster/connection_pool.h"
#include "cluster/coordinator.h"
#include "cluster/hash_ring.h"
#include "cluster/heartbeat.h"
#include "cluster/membership.h"
#include "config/config.h"
#include "network/tcp_server.h"
#include "storage/snapshot.h"
#include "storage/storage_engine.h"
#include "storage/wal.h"

#include "utils/logger.h"

#include <csignal>
#include <iostream>

static dkv::TCPServer* g_server = nullptr;

void signal_handler(int) {
    // Stop accepting new connections and drain in-flight requests first.
    // Heartbeat keeps running during drain so membership stays current.
    if (g_server) g_server->stop();
}

int main(int argc, char* argv[]) {
    dkv::Config cfg = dkv::parse_args(argc, argv);

    // Initialize logger before anything else
    dkv::Logger::instance().set_level(dkv::parse_log_level(cfg.log_level));
    dkv::Logger::instance().set_stream(&std::cout);

    dkv::print_config(cfg);

    // validate quorum invariant: W + R > N
    if (cfg.write_quorum + cfg.read_quorum <= cfg.replication_factor) {
        LOG_FATAL("Quorum invariant violated: W(" << cfg.write_quorum
                  << ") + R(" << cfg.read_quorum
                  << ") must be > N(" << cfg.replication_factor << ")");
        return 1;
    }

    // ── Parse cluster configuration ─────────────────────────────────────────
    auto cluster_entries = dkv::parse_cluster_config(cfg.cluster_conf);
    LOG_INFO("[BOOT] Loaded " << cluster_entries.size()
             << " nodes from " << cfg.cluster_conf);

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
        LOG_DEBUG("[BOOT] Ring: " << entry.name << " (id=" << id
                  << ") -> " << address);
    }

    LOG_INFO("[BOOT] Hash ring: " << ring.node_count() << " physical nodes, "
             << ring.size() << " virtual nodes");

    // ── Boot the node ───────────────────────────────────────────────────────
    LOG_INFO("[BOOT] Node " << cfg.node_id << " listening on port " << cfg.port);
    LOG_INFO("[BOOT] Quorum: W=" << cfg.write_quorum
             << " R=" << cfg.read_quorum << " N=" << cfg.replication_factor);

    // ── Initialize storage engine ───────────────────────────────────────────
    dkv::StorageEngine engine;

    // ── Open WAL and recover from disk ──────────────────────────────────────
    dkv::WAL wal;
    if (!wal.open(cfg.wal_dir, cfg.fsync_interval_ms, /*batch_ops=*/100)) {
        LOG_FATAL("Could not open WAL at " << cfg.wal_dir);
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
            LOG_INFO("[BOOT] Loaded snapshot at seq " << snapshot_seq
                     << " (" << snap_data->entries.size() << " entries)");
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
    LOG_INFO("[BOOT] WAL: " << records.size() << " total records, "
             << replayed << " replayed after snapshot");

    // ── Build coordinator with durability and quorum parameters ─────────────
    dkv::ConnectionPool conn_pool;
    dkv::Coordinator coordinator(engine, ring, conn_pool, cfg.node_id,
                                 &wal, cfg.snapshot_dir, cfg.snapshot_interval,
                                 cfg.replication_factor,
                                 cfg.write_quorum,
                                 cfg.read_quorum,
                                 cfg.hints_dir);


    // Phase 6: Build membership tracker
    dkv::Membership membership(3, static_cast<int>(cfg.heartbeat_timeout_ms));

    for (const auto& entry : cluster_entries) {
        uint32_t id = 0;
        for (char c : entry.name) {
            if (c >= '0' && c <= '9') id = id * 10 + static_cast<uint32_t>(c - '0');
        }
        if (id == 0) id = static_cast<uint32_t>(std::hash<std::string>{}(entry.name) & 0xFFFFFFFF);
        if (id == cfg.node_id) continue;
        std::string addr = entry.host + ":" + std::to_string(entry.port);
        membership.add_peer(id, addr);
    }

    membership.set_rejoin_callback([&coordinator](uint32_t node_id, const std::string& addr) {
        LOG_INFO("[MEMBERSHIP] Node " << node_id << " at " << addr << " is UP - replaying hints");
        coordinator.replay_hints_for(node_id, addr);
    });

    membership.set_down_callback([](uint32_t node_id, const std::string& addr) {
        LOG_WARN("[MEMBERSHIP] Node " << node_id << " at " << addr << " is DOWN");
    });

    // Wire membership into coordinator so DOWN nodes are skipped in quorum ops.
    coordinator.set_membership(&membership);

    // Phase 6: Start heartbeat
    dkv::Heartbeat heartbeat(membership, cfg.node_id,
                             static_cast<int>(cfg.heartbeat_interval_ms), 500);
    heartbeat.start();
    LOG_INFO("[BOOT] Heartbeat started (interval=" << cfg.heartbeat_interval_ms
             << "ms, timeout=" << cfg.heartbeat_timeout_ms << "ms)");

    // Create TCP server in cluster mode (routes through coordinator)
    dkv::TCPServer server(engine, coordinator, cfg.port,
                          cfg.worker_threads, cfg.node_id);
    g_server = &server;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    LOG_INFO("[BOOT] Server running in cluster mode");
    server.run();
    heartbeat.stop();

    // ── Graceful shutdown: flush WAL ─────────────────────────────────────────
    wal.sync();
    wal.close();
    LOG_INFO("[SHUTDOWN] WAL flushed and closed");

    return 0;
}
