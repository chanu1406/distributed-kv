#pragma once

#include "storage/storage_engine.h"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace dkv {

/// A pending write for a replica that was DOWN at write time (§9.D).
struct Hint {
    std::string target_address;  // "host:port" of the intended replica
    uint32_t    target_node_id;
    std::string key;
    std::string value;           // empty for is_del == true
    bool        is_del;
    Version     version;         // the exact version the coordinator chose
};

/// Thread-safe store for hinted handoff.
///
/// When a quorum write cannot reach a replica (connection refused / timeout),
/// the coordinator stores a Hint here.  Phase 6 calls replay_for() once the
/// node is seen as UP again so the hints are delivered and then deleted.
///
/// Persistence: hints are appended to "<hints_dir>/hints_<target_node_id>.dat"
/// in a simple binary format so they survive coordinator crashes.
class HintStore {
public:
    /// @param hints_dir  Directory for hint files.  Empty = in-memory only.
    explicit HintStore(const std::string& hints_dir = "");

    /// Persist a hint (and keep it in memory for fast replay).
    void store(const Hint& hint);

    /// Return all pending hints for the given target node.
    std::vector<Hint> get_hints_for(uint32_t target_node_id) const;

    /// Remove all hints for the given target node (call after successful replay).
    void clear_hints_for(uint32_t target_node_id);

    /// Total number of pending hints across all nodes.
    size_t size() const;

    /// Load hints from disk (call once on startup to recover across crashes).
    void load();

    // Non-copyable
    HintStore(const HintStore&) = delete;
    HintStore& operator=(const HintStore&) = delete;

private:
    std::string hints_dir_;

    mutable std::mutex mutex_;
    // target_node_id → pending hints
    std::unordered_map<uint32_t, std::vector<Hint>> hints_;

    /// Append a single hint to the on-disk file for target_node_id.
    /// Called while mutex_ is NOT held (file I/O outside the lock).
    void append_to_disk(const Hint& hint) const;

    /// File path for a given target node's hints.
    std::string hint_file_path(uint32_t target_node_id) const;

    /// Parse all hints from a single hint file.
    std::vector<Hint> load_file(const std::string& path) const;
};

}  // namespace dkv
