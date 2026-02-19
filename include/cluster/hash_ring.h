#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace dkv {

/// Information about a physical node in the cluster.
struct NodeInfo {
    uint32_t    node_id = 0;
    std::string address;        // "ip:port"
};

/// Consistent hash ring with virtual nodes using MurmurHash3.
///
/// Each physical node is mapped to `num_vnodes` positions on a 64-bit
/// hash ring.  Key lookups walk clockwise (via upper_bound + wrap) to
/// find the owning node.
class HashRing {
public:
    /// Add a physical node with `num_vnodes` virtual nodes.
    void add_node(uint32_t node_id, const std::string& address,
                  uint32_t num_vnodes = 128);

    /// Remove all virtual nodes belonging to a physical node.
    void remove_node(uint32_t node_id);

    /// Lookup the node that owns a given key.
    /// Returns std::nullopt if the ring is empty.
    std::optional<NodeInfo> get_node(const std::string& key) const;

    /// Return up to `count` distinct physical nodes clockwise from the
    /// key's position.  Used for replication (replica set selection).
    std::vector<NodeInfo> get_replica_nodes(const std::string& key,
                                            size_t count) const;

    /// Number of virtual nodes on the ring.
    size_t size() const { return ring_.size(); }

    /// Number of physical nodes registered.
    size_t node_count() const { return nodes_.size(); }

private:
    /// The ring: hash position → node info.
    std::map<uint64_t, NodeInfo> ring_;

    /// Track which physical nodes are registered (node_id → address).
    std::unordered_map<uint32_t, std::string> nodes_;
};

}  // namespace dkv
