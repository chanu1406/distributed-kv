#pragma once

#include "cluster/connection_pool.h"
#include "cluster/hash_ring.h"
#include "cluster/membership.h"
#include "network/protocol.h"
#include "network/thread_pool.h"
#include "replication/hint_store.h"
#include "storage/snapshot.h"
#include "storage/storage_engine.h"
#include "storage/wal.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace dkv {

/// Routes commands to the correct node based on the hash ring.
///
/// Phase 4 (single-owner routing) is extended in Phase 5 with quorum
/// scatter-gather: SET/DEL scatter to all N replicas in parallel and wait
/// for W acknowledgements; GET sends to R replicas and returns the highest-
/// version value with async read repair for stale replicas.
///
/// PING is always handled locally.
/// FWD frames have their hop counter decremented; ROUTING_LOOP is returned
/// if TTL reaches 0.
/// RSET/RDEL/RGET are internal replication commands executed locally always.
class Coordinator {
public:
    /// @param engine              Local storage engine.
    /// @param ring                Consistent hash ring (must outlive coordinator).
    /// @param pool                Connection pool for inter-node communication.
    /// @param node_id             This node's unique ID.
    /// @param wal                 Optional WAL for durability (nullptr = in-memory only).
    /// @param snapshot_dir        Directory for snapshot files ("" = disabled).
    /// @param snapshot_interval   Ops between snapshots.
    /// @param replication_factor  N — total replicas per key (default 1 = no replication).
    /// @param write_quorum        W — acks required for a successful write (default 1).
    /// @param read_quorum         R — replicas queried on a read (default 1).
    /// @param hints_dir           Directory for hint files ("" = in-memory only).
    Coordinator(StorageEngine& engine, HashRing& ring,
                ConnectionPool& pool, uint32_t node_id,
                WAL* wal = nullptr,
                const std::string& snapshot_dir = "",
                uint64_t snapshot_interval = 100000,
                uint32_t replication_factor = 1,
                uint32_t write_quorum = 1,
                uint32_t read_quorum = 1,
                const std::string& hints_dir = "");

    ~Coordinator();

    /// Handle a command: quorum-scatter for SET/DEL/GET, execute locally for
    /// RSET/RDEL/RGET/FWD, always local for PING.
    std::string handle_command(const Command& cmd);

    /// Called by Phase 6 heartbeat when a previously-DOWN node responds to a
    /// PING.  Replays all stored hints and removes them on success (§9.D).
    void replay_hints_for(uint32_t target_node_id,
                          const std::string& target_address);

    /// Register the cluster membership tracker.  When set, quorum_write will
    /// immediately store a hint (no TCP attempt) for DOWN replicas, and
    /// quorum_read will skip DOWN replicas rather than timing out on them.
    /// Safe to call before the first command arrives.  Raw pointer — the
    /// Membership object is owned by main() and outlives the coordinator.
    void set_membership(Membership* membership);

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

    // Quorum parameters (§9 of CONTEXT.md)
    uint32_t replication_factor_ = 1;
    uint32_t write_quorum_       = 1;
    uint32_t read_quorum_        = 1;

    // Hinted handoff store (§9.D of CONTEXT.md)
    HintStore hints_;

    // Optional Phase-6 membership tracker (nullptr = no DOWN-node awareness).
    Membership* membership_ = nullptr;

    // Monotonic timestamp: guarantees strictly increasing versions even when
    // two operations from this node land in the same millisecond (LWW fix).
    std::atomic<uint64_t> last_ts_{0};

    // ── Quorum thread pool (Task 2: replaces per-request thread spawns) ───────
    std::unique_ptr<ThreadPool> quorum_pool_;

    static constexpr uint32_t DEFAULT_HOPS = 2;

    // ── Background repair queue (Task 1: replaces detached thread) ───────────
    std::queue<std::function<void()>> repair_queue_;
    std::mutex                        repair_mutex_;
    std::condition_variable           repair_cv_;
    std::thread                       repair_thread_;
    std::atomic<bool>                 repair_running_{false};

    /// Worker function that drains repair_queue_ until repair_running_ = false.
    void repair_worker();

    // ── Quorum operations ────────────────────────────────────────────────────

    /// Scatter a SET or DEL to all N replicas; wait for W acks.
    /// Returns +OK or -ERR QUORUM_FAILED.
    std::string quorum_write(const std::string& key, const std::string& value,
                             bool is_del);

    /// Send GET to R replicas; return highest-version value.
    /// Triggers async read repair for stale replicas.
    std::string quorum_read(const std::string& key);

    // ── Inter-node helpers ───────────────────────────────────────────────────

    /// Send RSET or RDEL directly to a remote replica.
    /// Returns true if the replica acknowledged with +OK.
    bool send_replication_write(const std::string& address,
                                const std::string& key,
                                const std::string& value,
                                bool is_del, const Version& version);

    /// Result of a remote RGET call.
    struct RemoteGetResult {
        bool        ok    = false;  // connection + parse succeeded
        bool        found = false;
        std::string value;
        Version     version;
    };

    /// Send RGET to a remote replica and parse the versioned response.
    RemoteGetResult send_replication_read(const std::string& address,
                                          const std::string& key);

    /// Fire-and-forget async RSET to stale replicas (read repair, §9.C).
    void read_repair_async(const std::string& key, const std::string& value,
                           const Version& latest_ver,
                           std::vector<NodeInfo> stale_replicas);

    // ── Legacy / local execution ─────────────────────────────────────────────

    /// Execute a command locally on the storage engine.
    /// Handles SET, GET, DEL, PING, RSET, RDEL, RGET.
    std::string execute_local(const Command& cmd);

    /// Forward a command to a remote node (Phase 4 FWD mechanism).
    std::string forward_to(const std::string& address,
                           const std::string& inner_line, uint32_t hops);

    /// Serialise a Command back into its wire-format line (no trailing newline).
    static std::string serialize_command_line(const Command& cmd);

    /// Monotonic timestamp generator: returns a value strictly greater than
    /// any previous call, even within the same millisecond. Prevents the LWW
    /// edge case where SET and DEL at the same millisecond produce identical
    /// versions and the second operation is silently rejected.
    uint64_t next_ts();

    /// Trigger a snapshot if ops_since_snapshot_ >= snapshot_interval_.
    void maybe_snapshot();
};

}  // namespace dkv
