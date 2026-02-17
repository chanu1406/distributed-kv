#pragma once

#include <cstdint>
#include <string>

namespace dkv {

/// All configurable runtime parameters for a DKV node.
struct Config {
    // ── Identity ────────────────────────────────────────────────────────────
    uint32_t    node_id             = 1;
    uint16_t    port                = 7001;
    std::string cluster_conf        = "cluster.conf";

    // ── Replication ─────────────────────────────────────────────────────────
    uint32_t    replication_factor   = 3;
    uint32_t    write_quorum         = 2;
    uint32_t    read_quorum          = 2;

    // ── Hash Ring ───────────────────────────────────────────────────────────
    uint32_t    vnodes               = 128;

    // ── WAL & Snapshots ─────────────────────────────────────────────────────
    std::string wal_dir              = "./data/wal/";
    std::string snapshot_dir         = "./data/snapshots/";
    uint64_t    snapshot_interval    = 100000;   // ops between snapshots
    uint32_t    fsync_interval_ms    = 10;       // max ms between fsyncs

    // ── Threading ───────────────────────────────────────────────────────────
    uint32_t    worker_threads       = 4;

    // ── Cluster Health ──────────────────────────────────────────────────────
    uint32_t    heartbeat_interval_ms = 1000;
    uint32_t    heartbeat_timeout_ms  = 5000;
};

/// Parse command-line arguments into a Config struct.
/// Unrecognized flags are ignored with a warning printed to stderr.
/// @param argc  Argument count (from main).
/// @param argv  Argument values (from main).
/// @return      Populated Config struct.
Config parse_args(int argc, char* argv[]);

/// Print a summary of the active configuration to stdout.
void print_config(const Config& cfg);

}  // namespace dkv
