#pragma once

#include "storage/storage_engine.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace dkv {

/// Result of loading a snapshot from disk.
struct SnapshotData {
    uint64_t seq_no = 0;
    std::vector<std::pair<std::string, ValueEntry>> entries;
};

/// Snapshot serialization and recovery.
///
/// Binary format:
///   [Magic 4B "DKVS"] [SeqNo 8B] [EntryCount 4B]
///   foreach entry:
///     [Tombstone 1B] [KeyLen 4B] [Key] [ValLen 4B] [Value]
///     [Timestamp 8B] [NodeId 4B]
class Snapshot {
public:
    /// Serialize the entire StorageEngine state (including tombstones) to
    /// a file named `snapshot_<seq_no>.dat` in `directory`.
    /// Returns true on success.
    static bool save(const StorageEngine& engine, uint64_t seq_no,
                     const std::string& directory);

    /// Load a snapshot from a specific file path.
    static std::optional<SnapshotData> load(const std::string& filepath);

    /// Find the latest snapshot file in a directory.
    /// Returns the path, or std::nullopt if none found.
    static std::optional<std::string> find_latest(const std::string& directory);
};

}  // namespace dkv
