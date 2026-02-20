#include "cluster/cluster_config.h"
#include "cluster/connection_pool.h"
#include "cluster/coordinator.h"
#include "cluster/hash_ring.h"
#include "config/config.h"
#include "network/tcp_server.h"
#include "storage/storage_engine.h"

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

    // Initialize core components
    dkv::StorageEngine engine;
    dkv::ConnectionPool conn_pool;
    dkv::Coordinator coordinator(engine, ring, conn_pool, cfg.node_id);

    // Create TCP server in cluster mode (routes through coordinator)
    dkv::TCPServer server(engine, coordinator, cfg.port,
                          cfg.worker_threads, cfg.node_id);
    g_server = &server;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "[BOOT] Server running in cluster mode\n";
    server.run();
    return 0;
}
