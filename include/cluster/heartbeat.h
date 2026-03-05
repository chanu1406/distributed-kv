#pragma once

#include "cluster/membership.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace dkv {

/// Sends periodic PING messages to all peers and updates Membership.
///
/// A background thread wakes every `interval_ms` and sends a PING to
/// each registered peer via a raw TCP connection.  Results are fed into
/// the Membership object which owns the state machine.
///
/// The Coordinator's replay_hints_for() is called via the Membership
/// rejoin callback when a DOWN node returns UP.
class Heartbeat {
public:
    /// @param membership      Shared membership tracker (must outlive Heartbeat).
    /// @param node_id         This node's own id (used to skip self-pings).
    /// @param interval_ms     How often to ping each peer (ms).
    /// @param timeout_ms      Socket read/write timeout for PING (ms).
    Heartbeat(Membership& membership, uint32_t node_id,
              int interval_ms = 1000, int timeout_ms = 500);

    ~Heartbeat();

    /// Start the background ping thread.
    void start();

    /// Stop the background ping thread (blocks until thread joins).
    void stop();

    // Non-copyable
    Heartbeat(const Heartbeat&) = delete;
    Heartbeat& operator=(const Heartbeat&) = delete;

private:
    Membership& membership_;
    uint32_t    node_id_;
    int         interval_ms_;
    int         timeout_ms_;

    std::atomic<bool> running_{false};
    std::thread       thread_;

    /// Background loop.
    void run();

    /// Send one PING to `address` and return true if +PONG is received.
    bool ping_node(const std::string& address) const;
};

}  // namespace dkv
