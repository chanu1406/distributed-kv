#include "replication/hint_store.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>

namespace dkv {

// ── Binary serialisation helpers ─────────────────────────────────────────────

namespace {

void write_u32(std::ostream& os, uint32_t v) {
    os.write(reinterpret_cast<const char*>(&v), 4);
}
void write_u64(std::ostream& os, uint64_t v) {
    os.write(reinterpret_cast<const char*>(&v), 8);
}
void write_u8(std::ostream& os, uint8_t v) {
    os.write(reinterpret_cast<const char*>(&v), 1);
}
void write_str(std::ostream& os, const std::string& s) {
    uint32_t len = static_cast<uint32_t>(s.size());
    write_u32(os, len);
    os.write(s.data(), len);
}

bool read_u32(std::istream& is, uint32_t& v) {
    return static_cast<bool>(is.read(reinterpret_cast<char*>(&v), 4));
}
bool read_u64(std::istream& is, uint64_t& v) {
    return static_cast<bool>(is.read(reinterpret_cast<char*>(&v), 8));
}
bool read_u8(std::istream& is, uint8_t& v) {
    return static_cast<bool>(is.read(reinterpret_cast<char*>(&v), 1));
}
bool read_str(std::istream& is, std::string& s) {
    uint32_t len = 0;
    if (!read_u32(is, len)) return false;
    s.resize(len);
    return static_cast<bool>(is.read(s.data(), len));
}

}  // namespace

// ── HintStore public API ──────────────────────────────────────────────────────

HintStore::HintStore(const std::string& hints_dir)
    : hints_dir_(hints_dir) {}

void HintStore::store(const Hint& hint) {
    {
        std::lock_guard lock(mutex_);
        hints_[hint.target_node_id].push_back(hint);
    }
    // Append to disk outside the lock (file I/O can be slow).
    if (!hints_dir_.empty()) {
        append_to_disk(hint);
    }
}

std::vector<Hint> HintStore::get_hints_for(uint32_t target_node_id) const {
    std::lock_guard lock(mutex_);
    auto it = hints_.find(target_node_id);
    if (it == hints_.end()) return {};
    return it->second;
}

void HintStore::clear_hints_for(uint32_t target_node_id) {
    std::lock_guard lock(mutex_);
    hints_.erase(target_node_id);

    // Remove the on-disk file (best-effort).
    if (!hints_dir_.empty()) {
        std::string path = hint_file_path(target_node_id);
        std::error_code ec;
        std::filesystem::remove(path, ec);
    }
}

size_t HintStore::size() const {
    std::lock_guard lock(mutex_);
    size_t total = 0;
    for (const auto& [id, vec] : hints_) total += vec.size();
    return total;
}

void HintStore::load() {
    if (hints_dir_.empty()) return;

    std::error_code ec;
    if (!std::filesystem::exists(hints_dir_, ec)) return;

    for (const auto& entry : std::filesystem::directory_iterator(hints_dir_, ec)) {
        const std::string fname = entry.path().filename().string();
        // Only process files named "hints_<id>.dat"
        if (fname.rfind("hints_", 0) != 0) continue;
        if (fname.size() < 11) continue;  // "hints_X.dat" minimum

        auto hints = load_file(entry.path().string());
        std::lock_guard lock(mutex_);
        for (auto& h : hints) {
            hints_[h.target_node_id].push_back(std::move(h));
        }
    }
}

// ── Private helpers ──────────────────────────────────────────────────────────

void HintStore::append_to_disk(const Hint& hint) const {
    std::string path = hint_file_path(hint.target_node_id);

    // Ensure directory exists.
    std::error_code ec;
    std::filesystem::create_directories(hints_dir_, ec);

    std::ofstream f(path, std::ios::binary | std::ios::app);
    if (!f) {
        std::cerr << "[HINT] Cannot open hint file: " << path << "\n";
        return;
    }

    // Record format (all fields little-endian on this platform):
    //   [target_node_id u32][addr_len u32][addr bytes]
    //   [key_len u32][key bytes][val_len u32][val bytes]
    //   [timestamp_ms u64][node_id u32][is_del u8]
    write_u32(f, hint.target_node_id);
    write_str(f, hint.target_address);
    write_str(f, hint.key);
    write_str(f, hint.value);
    write_u64(f, hint.version.timestamp_ms);
    write_u32(f, hint.version.node_id);
    write_u8 (f, hint.is_del ? 1 : 0);
}

std::string HintStore::hint_file_path(uint32_t target_node_id) const {
    return hints_dir_ + "/hints_" + std::to_string(target_node_id) + ".dat";
}

std::vector<Hint> HintStore::load_file(const std::string& path) const {
    std::vector<Hint> result;
    std::ifstream f(path, std::ios::binary);
    if (!f) return result;

    while (f.peek() != EOF) {
        Hint h;
        uint8_t is_del_byte = 0;

        if (!read_u32(f, h.target_node_id))   break;
        if (!read_str(f, h.target_address))    break;
        if (!read_str(f, h.key))               break;
        if (!read_str(f, h.value))             break;
        if (!read_u64(f, h.version.timestamp_ms)) break;
        if (!read_u32(f, h.version.node_id))   break;
        if (!read_u8 (f, is_del_byte))         break;

        h.is_del = (is_del_byte != 0);
        result.push_back(std::move(h));
    }

    return result;
}

}  // namespace dkv
