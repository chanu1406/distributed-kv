#include "cluster/membership.h"

#include <stdexcept>

namespace dkv {

Membership::Membership(int suspect_threshold, int down_threshold_ms)
    : suspect_threshold_(suspect_threshold),
      down_threshold_ms_(down_threshold_ms) {}

void Membership::add_peer(uint32_t node_id, const std::string& address) {
    std::lock_guard<std::mutex> lock(mutex_);
    NodeStatus s;
    s.node_id   = node_id;
    s.address   = address;
    s.state     = NodeState::UP;
    s.miss_count = 0;
    s.last_seen  = std::chrono::steady_clock::now();
    peers_[node_id] = std::move(s);
}

void Membership::record_success(uint32_t node_id) {
    DownCallback   down_cb;
    RejoinCallback rejoin_cb;
    NodeState old_state;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = peers_.find(node_id);
        if (it == peers_.end()) return;

        old_state         = it->second.state;
        it->second.miss_count = 0;
        it->second.last_seen  = std::chrono::steady_clock::now();

        if (it->second.state != NodeState::UP) {
            it->second.state = NodeState::UP;
            rejoin_cb = rejoin_cb_;
        }
    }

    if (rejoin_cb && old_state == NodeState::DOWN) {
        std::string addr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = peers_.find(node_id);
            if (it != peers_.end()) addr = it->second.address;
        }
        rejoin_cb(node_id, addr);
    }
}

void Membership::record_failure(uint32_t node_id) {
    DownCallback   fire_down_cb;
    std::string    fire_down_addr;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = peers_.find(node_id);
        if (it == peers_.end()) return;

        // Already DOWN — nothing to do
        if (it->second.state == NodeState::DOWN) return;

        it->second.miss_count++;

        if (it->second.state == NodeState::UP &&
            it->second.miss_count >= suspect_threshold_) {
            it->second.state = NodeState::SUSPECTED;
        }

        if (it->second.state == NodeState::SUSPECTED) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - it->second.last_seen).count();
            if (elapsed_ms >= down_threshold_ms_) {
                it->second.state = NodeState::DOWN;
                fire_down_cb   = down_cb_;
                fire_down_addr = it->second.address;
            }
        }
    }

    if (fire_down_cb) {
        fire_down_cb(node_id, fire_down_addr);
    }
}

NodeState Membership::get_state(uint32_t node_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = peers_.find(node_id);
    if (it == peers_.end()) return NodeState::UP;  // unknown = assume up
    return it->second.state;
}

bool Membership::is_available(uint32_t node_id) const {
    return get_state(node_id) != NodeState::DOWN;
}

std::vector<uint32_t> Membership::down_nodes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<uint32_t> result;
    for (const auto& [id, status] : peers_) {
        if (status.state == NodeState::DOWN) result.push_back(id);
    }
    return result;
}

std::vector<NodeStatus> Membership::all_peers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<NodeStatus> result;
    result.reserve(peers_.size());
    for (const auto& [id, status] : peers_) {
        result.push_back(status);
    }
    return result;
}

}  // namespace dkv
