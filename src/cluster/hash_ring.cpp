#include "cluster/hash_ring.h"
#include "utils/murmurhash3.h"

#include <iostream>

namespace dkv {

void HashRing::add_node(uint32_t node_id, const std::string& address,
                        uint32_t num_vnodes) {
    nodes_[node_id] = address;

    NodeInfo info{node_id, address};

    for (uint32_t i = 0; i < num_vnodes; i++) {
        // Hash "node_id:vnode_index" to get the ring position
        std::string vnode_key = std::to_string(node_id) + ":" + std::to_string(i);
        uint64_t hash = murmurhash3(vnode_key);

        if (ring_.count(hash)) {
            std::cerr << "[RING] Hash collision at position " << hash
                      << " for node " << node_id << " vnode " << i
                      << " — skipping\n";
            continue;
        }

        ring_[hash] = info;
    }
}

void HashRing::remove_node(uint32_t node_id) {
    // Erase all ring entries belonging to this node
    for (auto it = ring_.begin(); it != ring_.end(); ) {
        if (it->second.node_id == node_id) {
            it = ring_.erase(it);
        } else {
            ++it;
        }
    }

    nodes_.erase(node_id);
}

std::optional<NodeInfo> HashRing::get_node(const std::string& key) const {
    if (ring_.empty()) return std::nullopt;

    uint64_t hash = murmurhash3(key);

    // Walk clockwise: find the first node with position > hash
    auto it = ring_.upper_bound(hash);
    if (it == ring_.end()) {
        // Wrap around to the beginning of the ring
        it = ring_.begin();
    }

    return it->second;
}

std::vector<NodeInfo> HashRing::get_replica_nodes(const std::string& key,
                                                   size_t count) const {
    std::vector<NodeInfo> result;
    if (ring_.empty()) return result;

    // Can't return more distinct physical nodes than exist
    size_t max_nodes = nodes_.size();
    if (count > max_nodes) count = max_nodes;

    uint64_t hash = murmurhash3(key);
    auto it = ring_.upper_bound(hash);
    if (it == ring_.end()) it = ring_.begin();

    // Walk clockwise, collecting distinct physical nodes
    size_t visited = 0;
    while (result.size() < count) {
        // Check if we already have this physical node
        bool already_have = false;
        for (const auto& n : result) {
            if (n.node_id == it->second.node_id) {
                already_have = true;
                break;
            }
        }

        if (!already_have) {
            result.push_back(it->second);
        }

        ++it;
        if (it == ring_.end()) it = ring_.begin();

        ++visited;
        if (visited >= ring_.size()) break;  // full loop, no more unique nodes
    }

    return result;
}

}  // namespace dkv
