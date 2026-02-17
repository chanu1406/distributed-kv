#include "config/config.h"

#include <cstring>
#include <iostream>
#include <string>

namespace dkv {

Config parse_args(int argc, char* argv[]) {
    Config cfg;

    for (int i = 1; i < argc; i++) {
        auto match = [&](const char* flag) -> bool {
            return std::strcmp(argv[i], flag) == 0 && (i + 1) < argc;
        };

        if (match("--port")) {
            cfg.port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (match("--node-id")) {
            cfg.node_id = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (match("--cluster-conf")) {
            cfg.cluster_conf = argv[++i];
        } else if (match("--replication-factor")) {
            cfg.replication_factor = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (match("--write-quorum")) {
            cfg.write_quorum = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (match("--read-quorum")) {
            cfg.read_quorum = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (match("--vnodes")) {
            cfg.vnodes = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (match("--wal-dir")) {
            cfg.wal_dir = argv[++i];
        } else if (match("--snapshot-dir")) {
            cfg.snapshot_dir = argv[++i];
        } else if (match("--snapshot-interval")) {
            cfg.snapshot_interval = std::stoull(argv[++i]);
        } else if (match("--fsync-interval-ms")) {
            cfg.fsync_interval_ms = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (match("--worker-threads")) {
            cfg.worker_threads = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (match("--heartbeat-interval-ms")) {
            cfg.heartbeat_interval_ms = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (match("--heartbeat-timeout-ms")) {
            cfg.heartbeat_timeout_ms = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            std::cout << "Usage: dkv_node [OPTIONS]\n\n"
                      << "Options:\n"
                      << "  --port <PORT>                Listen port (default: 7001)\n"
                      << "  --node-id <ID>               Unique node identifier (default: 1)\n"
                      << "  --cluster-conf <PATH>        Cluster config file (default: cluster.conf)\n"
                      << "  --replication-factor <N>     Replication factor (default: 3)\n"
                      << "  --write-quorum <W>           Write quorum (default: 2)\n"
                      << "  --read-quorum <R>            Read quorum (default: 2)\n"
                      << "  --vnodes <V>                 Virtual nodes per physical node (default: 128)\n"
                      << "  --wal-dir <PATH>             WAL directory (default: ./data/wal/)\n"
                      << "  --snapshot-dir <PATH>        Snapshot directory (default: ./data/snapshots/)\n"
                      << "  --snapshot-interval <OPS>    Ops between snapshots (default: 100000)\n"
                      << "  --fsync-interval-ms <MS>     Max ms between fsyncs (default: 10)\n"
                      << "  --worker-threads <N>         Worker threads (default: 4)\n"
                      << "  --heartbeat-interval-ms <MS> Heartbeat period (default: 1000)\n"
                      << "  --heartbeat-timeout-ms <MS>  Down detection timeout (default: 5000)\n"
                      << "  -h, --help                   Show this help\n";
            std::exit(0);
        } else {
            std::cerr << "[WARN] Unknown flag: " << argv[i] << "\n";
        }
    }

    return cfg;
}

void print_config(const Config& cfg) {
    std::cout << "┌──────────────────────────────────────────┐\n"
              << "│         DKV Node Configuration           │\n"
              << "├──────────────────────────────────────────┤\n"
              << "│  Node ID:              " << cfg.node_id << "\n"
              << "│  Port:                 " << cfg.port << "\n"
              << "│  Cluster Config:       " << cfg.cluster_conf << "\n"
              << "│  Replication Factor:   " << cfg.replication_factor << "\n"
              << "│  Write Quorum (W):     " << cfg.write_quorum << "\n"
              << "│  Read Quorum (R):      " << cfg.read_quorum << "\n"
              << "│  Virtual Nodes:        " << cfg.vnodes << "\n"
              << "│  WAL Directory:        " << cfg.wal_dir << "\n"
              << "│  Snapshot Directory:   " << cfg.snapshot_dir << "\n"
              << "│  Snapshot Interval:    " << cfg.snapshot_interval << " ops\n"
              << "│  Fsync Interval:       " << cfg.fsync_interval_ms << " ms\n"
              << "│  Worker Threads:       " << cfg.worker_threads << "\n"
              << "│  Heartbeat Interval:   " << cfg.heartbeat_interval_ms << " ms\n"
              << "│  Heartbeat Timeout:    " << cfg.heartbeat_timeout_ms << " ms\n"
              << "└──────────────────────────────────────────┘\n";
}

}  // namespace dkv
