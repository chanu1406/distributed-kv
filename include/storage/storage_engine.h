#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <shared_mutex>
#include <array>
#include <unordered_map>
#include <vector>

namespace dkv {

/// Logical timestamp used for Last-Write-Wins conflict resolution.
struct Version {
    uint64_t timestamp_ms = 0;  // Milliseconds since epoch
    uint32_t node_id      = 0;  // Tiebreaker: higher node_id wins
};

/// Returns true if `a` is strictly newer than `b` under LWW rules.
inline bool is_newer(const Version& a, const Version& b) {
    if (a.timestamp_ms != b.timestamp_ms)
        return a.timestamp_ms > b.timestamp_ms;
    return a.node_id > b.node_id;
}

/// A single value stored in the engine.  Tombstoned entries preserve the
/// version so that read-repair cannot accidentally resurrect deleted keys.
struct ValueEntry {
    bool        is_tombstone = false;
    std::string value;
    Version     version;
};

/// Result of a GET request.
struct GetResult {
    bool  found = false;       // true if key exists and is NOT tombstoned
    std::string value;
    Version     version;
};

/// Thread-safe, sharded in-memory key-value store with LWW versioning.
class StorageEngine {
public:
    /// Retrieve a key.  Returns found=false for missing keys and tombstones.
    GetResult get(const std::string& key) const;

    /// Insert or update a key.  Applies LWW — only writes if `version` is
    /// newer than the existing entry (or if the key doesn't exist).
    /// Returns true if the write was applied.
    bool set(const std::string& key, const std::string& value,
             const Version& version);

    /// Tombstone-delete a key.  Applies LWW — only tombstones if `version`
    /// is newer than the existing entry.
    /// Returns true if the tombstone was applied.
    bool del(const std::string& key, const Version& version);

    /// Return a snapshot of every entry (including tombstones).
    /// Used by the Snapshot module for serialization.
    std::vector<std::pair<std::string, ValueEntry>> all_entries() const;

private:
    static constexpr int NUM_SHARDS = 32;

    struct Shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, ValueEntry> data;
    };

    std::array<Shard, NUM_SHARDS> shards_;

    /// Determine which shard a key belongs to.
    size_t shard_index(const std::string& key) const;
};

}  // namespace dkv
