#include "storage/storage_engine.h"
#include "utils/murmurhash3.h"

#include <mutex>

namespace dkv {

size_t StorageEngine::shard_index(const std::string& key) const {
    return static_cast<size_t>(murmurhash3(key) % NUM_SHARDS);
}

GetResult StorageEngine::get(const std::string& key) const {
    const auto& shard = shards_[shard_index(key)];
    std::shared_lock lock(shard.mutex);

    auto it = shard.data.find(key);
    if (it == shard.data.end() || it->second.is_tombstone) {
        return {};  // found=false
    }

    return {true, it->second.value, it->second.version};
}

bool StorageEngine::set(const std::string& key, const std::string& value,
                        const Version& version) {
    auto& shard = shards_[shard_index(key)];
    std::unique_lock lock(shard.mutex);

    auto it = shard.data.find(key);
    if (it != shard.data.end() && !is_newer(version, it->second.version)) {
        return false;  // existing entry is same age or newer — reject
    }

    shard.data[key] = ValueEntry{false, value, version};
    return true;
}

bool StorageEngine::del(const std::string& key, const Version& version) {
    auto& shard = shards_[shard_index(key)];
    std::unique_lock lock(shard.mutex);

    auto it = shard.data.find(key);
    if (it != shard.data.end() && !is_newer(version, it->second.version)) {
        return false;  // existing entry is same age or newer — reject
    }

    // Write tombstone instead of erasing.  Preserves version for read repair.
    shard.data[key] = ValueEntry{true, "", version};
    return true;
}

std::vector<std::pair<std::string, ValueEntry>>
StorageEngine::all_entries() const {
    std::vector<std::pair<std::string, ValueEntry>> result;

    for (const auto& shard : shards_) {
        std::shared_lock lock(shard.mutex);
        for (const auto& [k, v] : shard.data) {
            result.emplace_back(k, v);
        }
    }

    return result;
}

}  // namespace dkv
