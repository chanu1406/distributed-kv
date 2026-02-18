#include "storage/snapshot.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <regex>

namespace dkv {

namespace {

constexpr uint8_t MAGIC[4] = {'D', 'K', 'V', 'S'};

void write_u32(std::ofstream& out, uint32_t val) {
    out.write(reinterpret_cast<const char*>(&val), 4);
}

void write_u64(std::ofstream& out, uint64_t val) {
    out.write(reinterpret_cast<const char*>(&val), 8);
}

uint32_t read_u32(std::ifstream& in) {
    uint32_t v = 0;
    in.read(reinterpret_cast<char*>(&v), 4);
    return v;
}

uint64_t read_u64(std::ifstream& in) {
    uint64_t v = 0;
    in.read(reinterpret_cast<char*>(&v), 8);
    return v;
}

}  // namespace

bool Snapshot::save(const StorageEngine& engine, uint64_t seq_no,
                    const std::string& directory) {
    std::filesystem::create_directories(directory);

    std::string filepath = directory + "/snapshot_" + std::to_string(seq_no) + ".dat";
    std::ofstream out(filepath, std::ios::binary);
    if (!out) {
        std::cerr << "[SNAPSHOT] Failed to create " << filepath << "\n";
        return false;
    }

    auto entries = engine.all_entries();

    // Header
    out.write(reinterpret_cast<const char*>(MAGIC), 4);
    write_u64(out, seq_no);
    write_u32(out, static_cast<uint32_t>(entries.size()));

    // Entries
    for (const auto& [key, entry] : entries) {
        uint8_t tombstone = entry.is_tombstone ? 1 : 0;
        out.write(reinterpret_cast<const char*>(&tombstone), 1);

        write_u32(out, static_cast<uint32_t>(key.size()));
        out.write(key.data(), static_cast<std::streamsize>(key.size()));

        write_u32(out, static_cast<uint32_t>(entry.value.size()));
        out.write(entry.value.data(), static_cast<std::streamsize>(entry.value.size()));

        write_u64(out, entry.version.timestamp_ms);
        write_u32(out, entry.version.node_id);
    }

    out.flush();
    return out.good();
}

std::optional<SnapshotData> Snapshot::load(const std::string& filepath) {
    std::ifstream in(filepath, std::ios::binary);
    if (!in) {
        std::cerr << "[SNAPSHOT] Failed to open " << filepath << "\n";
        return std::nullopt;
    }

    // Verify magic bytes
    uint8_t magic[4];
    in.read(reinterpret_cast<char*>(magic), 4);
    if (std::memcmp(magic, MAGIC, 4) != 0) {
        std::cerr << "[SNAPSHOT] Invalid magic bytes in " << filepath << "\n";
        return std::nullopt;
    }

    SnapshotData data;
    data.seq_no = read_u64(in);
    uint32_t count = read_u32(in);

    data.entries.reserve(count);
    for (uint32_t i = 0; i < count; i++) {
        uint8_t tombstone = 0;
        in.read(reinterpret_cast<char*>(&tombstone), 1);

        uint32_t key_len = read_u32(in);
        std::string key(key_len, '\0');
        in.read(key.data(), key_len);

        uint32_t val_len = read_u32(in);
        std::string value(val_len, '\0');
        in.read(value.data(), val_len);

        uint64_t timestamp_ms = read_u64(in);
        uint32_t node_id      = read_u32(in);

        if (!in.good()) {
            std::cerr << "[SNAPSHOT] Truncated entry at index " << i << "\n";
            return std::nullopt;
        }

        ValueEntry entry;
        entry.is_tombstone = (tombstone != 0);
        entry.value        = std::move(value);
        entry.version      = {timestamp_ms, node_id};

        data.entries.emplace_back(std::move(key), std::move(entry));
    }

    return data;
}

std::optional<std::string> Snapshot::find_latest(const std::string& directory) {
    if (!std::filesystem::exists(directory)) {
        return std::nullopt;
    }

    uint64_t    max_seq = 0;
    std::string best_path;

    // Match files like: snapshot_<number>.dat
    std::regex pattern(R"(snapshot_(\d+)\.dat)");

    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        if (!entry.is_regular_file()) continue;

        std::string filename = entry.path().filename().string();
        std::smatch match;
        if (std::regex_match(filename, match, pattern)) {
            uint64_t seq = std::stoull(match[1].str());
            if (seq > max_seq || best_path.empty()) {
                max_seq   = seq;
                best_path = entry.path().string();
            }
        }
    }

    if (best_path.empty()) {
        return std::nullopt;
    }

    return best_path;
}

}  // namespace dkv
