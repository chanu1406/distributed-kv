#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dkv {

/// A single entry from a cluster.conf file.
struct NodeEntry {
    std::string name;       // e.g. "node1"
    std::string host;       // e.g. "127.0.0.1"
    uint16_t    port = 0;   // e.g. 7001
};

/// Parse a cluster configuration file.
///
/// Expected format (one entry per line):
///   <name> <host>:<port>
///
/// Lines starting with '#' and blank lines are skipped.
/// Malformed lines are skipped with a warning to stderr.
std::vector<NodeEntry> parse_cluster_config(const std::string& filepath);

}  // namespace dkv
