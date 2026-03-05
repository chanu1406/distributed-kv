#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace dkv {

enum class NodeState {
    UP,
    SUSPECTED,
    DOWN,
};

struct NodeStatus {
    uint32_t    node_id    = 0;
    std::string address;           // "host:port"
    NodeState   state      = NodeState::UP;
    int         miss_count = 0;    // consecutive missed PINGs
    std::chrono::steady_clock::time_point last_seen;
};

/// Tracks the liveness state of all peer nodes.
///
/// State machine per node (section 8.B):
///   UP        -> SUSPECTED  after `suspect_threshold` consecutive missed PINGs
///   SUSPECTED -> DOWN       after `down_threshold_ms` total ms without response
///   DOWN      -> UP         when a PING response is received
///
/// Thread-safe.
class Membership {
public:
    using DownCallback   = std::function<void(uint32_t node_id, const std::string& address)>;
    using RejoinCallback = std::function<void(uint32_t node_id, const std::string& address)>;

    explicit Membership(int suspect_threshold = 3, int down_threshold_ms = 5000);

    void add_peer(uint32_t node_id, const std::string& address);
    void record_success(uint32_t node_id);
    void record_failure(uint32_t node_id);

    NodeState get_state(uint32_t node_id) const;
    bool is_available(uint32_t node_id) const;
    std::vector<uint32_t> down_nodes() const;
    std::vector<NodeStatus> all_peers() const;

    void set_down_callback(DownCallback cb)    { down_cb_   = std::move(cb); }
    void set_rejoin_callback(RejoinCallback cb) { rejoin_cb_ = std::move(cb); }

private:
    int suspect_threshold_;
    int down_threshold_ms_;

    mutable std::mutex                        mutex_;
    std::unordered_map<uint32_t, NodeStatus>  peers_;

    DownCallback   down_cb_;
    RejoinCallback rejoin_cb_;
};

}  // namespace dkv
