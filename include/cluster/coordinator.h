#pragma once

#include "cluster/connection_pool.h"
#include "cluster/hash_ring.h"
#include "network/protocol.h"
#include "storage/snapshot.h"
#include "storage/storage_engine.h"
#include "storage/wal.h"

#include <atomic>
#include <cstdint>
#include <string>

namespace dkv {

/// Routes commands to the correct node based on the hash ring.
///
/// For keys owned by this node, executes locally on the StorageEngine.
/// For keys owned by remote nodes, forwards via the ConnectionPool.
/// PING is always handled locally.
/// FWD frames have their hop counter decremented; ROUTING_LOOP is returned
/// if TTL reaches 0.
class Coordinator {
public:
    /// @param engine   Local storage engine.
    /// @param ring     Consistent hash ring (must outlive coordinator).
    /// @param pool     Connection pool for inter-node communication.
    /// @param node_id  This node's unique ID.
    Coordinator(StorageEngine& engine, HashRing& ring,
                ConnectionPool& pool, uint32_t node_id,
                WAL* wal = nullptr,
                const std::string& snapshot_dir = "",
                uint64_t snapshot_interval = 100000);

    /// Handle a command: route locally or forward to the correct node.
    /// Returns the response string to send back to the client.
    std::string handle_command(const Command& cmd);

    // Non-copyable
    Coordinator(const Coordinator&) = delete;
    Coordinator& operator=(const Coordinator&) = delete;

private:
    StorageEngine&  engine_;
    HashRing&       ring_;
    ConnectionPool& pool_;
    uint32_t        node_id_;

    // Durability (optional — nullptr means in-memory only)
    WAL*            wal_ = nullptr;
    std::string     snapshot_dir_;
    uint64_t        snapshot_interval_ = 100000;
    std::atomic<uint64_t> ops_since_snapshot_{0};

    static constexpr uint32_t DEFAULT_HOPS = 2;

    /// Execute a command locally on the storage engine.
    std::string execute_local(const Command& cmd);

    /// Forward a command to a remote node and return its response.
    std::string forward_to(const std::string& address,
                           const std::string& inner_line, uint32_t hops);

    /// Build the wire-format line for a command (without trailing newline).
    /// Used to construct FWD inner_line from a parsed Command.
    static std::string serialize_command_line(const Command& cmd);

    /// Trigger a snapshot if ops_since_snapshot_ >= snapshot_interval_.
    void maybe_snapshot();
};

}  // namespace dkv
