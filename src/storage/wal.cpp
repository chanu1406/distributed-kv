#include "storage/wal.h"
#include "utils/crc32.h"

#include <cstring>
#include <filesystem>
#include <iostream>

#include <fcntl.h>
#include <unistd.h>

namespace dkv {

// ── Serialization helpers ────────────────────────────────────────────────────

namespace {

void write_u32(std::vector<uint8_t>& buf, uint32_t val) {
    buf.push_back(static_cast<uint8_t>(val & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
}

void write_u64(std::vector<uint8_t>& buf, uint64_t val) {
    for (int i = 0; i < 8; i++) {
        buf.push_back(static_cast<uint8_t>((val >> (i * 8)) & 0xFF));
    }
}

uint32_t read_u32(const uint8_t* p) {
    uint32_t v = 0;
    std::memcpy(&v, p, 4);
    return v;
}

uint64_t read_u64(const uint8_t* p) {
    uint64_t v = 0;
    std::memcpy(&v, p, 8);
    return v;
}

}  // namespace

// ── WAL public interface ─────────────────────────────────────────────────────

bool WAL::open(const std::string& directory) {
    std::filesystem::create_directories(directory);
    filepath_ = directory + "/wal.bin";

    fd_ = ::open(filepath_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        std::cerr << "[WAL] Failed to open " << filepath_ << "\n";
        return false;
    }

    return true;
}

uint64_t WAL::append(const WalRecord& record) {
    std::lock_guard lock(mutex_);

    WalRecord rec = record;
    rec.seq_no = next_seq_no_++;

    auto buf = serialize(rec);
    ::write(fd_, buf.data(), buf.size());

    return rec.seq_no;
}

std::vector<WalRecord> WAL::recover() {
    std::vector<WalRecord> records;

    // Read entire file into memory for simplicity
    off_t file_size = ::lseek(fd_, 0, SEEK_END);
    if (file_size <= 0) {
        return records;
    }

    std::vector<uint8_t> data(static_cast<size_t>(file_size));
    ::lseek(fd_, 0, SEEK_SET);
    ssize_t bytes_read = ::read(fd_, data.data(), data.size());
    if (bytes_read <= 0) {
        return records;
    }

    // Seek back to end for future appends
    ::lseek(fd_, 0, SEEK_END);

    // Parse records sequentially, halt at first invalid CRC
    size_t offset = 0;
    while (offset < static_cast<size_t>(bytes_read)) {
        WalRecord rec;
        size_t consumed = 0;

        if (!deserialize(data.data() + offset,
                         static_cast<size_t>(bytes_read) - offset,
                         rec, consumed)) {
            std::cerr << "[WAL] Recovery halted at offset " << offset
                      << " (invalid CRC or truncated record)\n";
            break;
        }

        records.push_back(std::move(rec));
        offset += consumed;

        // Track highest seq_no for future appends
        if (rec.seq_no >= next_seq_no_) {
            next_seq_no_ = rec.seq_no + 1;
        }
    }

    return records;
}

void WAL::sync() {
    if (fd_ >= 0) {
        ::fsync(fd_);
    }
}

void WAL::close() {
    if (fd_ >= 0) {
        ::fsync(fd_);
        ::close(fd_);
        fd_ = -1;
    }
}

// ── Serialization ────────────────────────────────────────────────────────────

std::vector<uint8_t> WAL::serialize(const WalRecord& record) {
    // Layout: [CRC32 4B] [payload...]
    // Payload: [SeqNo 8B] [Timestamp 8B] [OpType 1B]
    //          [KeyLen 4B] [Key] [ValLen 4B] [Value]

    std::vector<uint8_t> payload;
    payload.reserve(64);

    write_u64(payload, record.seq_no);
    write_u64(payload, record.timestamp_ms);
    payload.push_back(static_cast<uint8_t>(record.op_type));
    write_u32(payload, static_cast<uint32_t>(record.key.size()));
    payload.insert(payload.end(), record.key.begin(), record.key.end());
    write_u32(payload, static_cast<uint32_t>(record.value.size()));
    payload.insert(payload.end(), record.value.begin(), record.value.end());

    // Compute CRC32 over the payload
    uint32_t checksum = crc32(payload.data(), payload.size());

    // Assemble final buffer: [CRC32] [payload]
    std::vector<uint8_t> buf;
    buf.reserve(4 + payload.size());
    write_u32(buf, checksum);
    buf.insert(buf.end(), payload.begin(), payload.end());

    return buf;
}

bool WAL::deserialize(const uint8_t* data, size_t len,
                      WalRecord& out, size_t& bytes_consumed) {
    // Minimum record size: 4 (CRC) + 8 (seq) + 8 (ts) + 1 (op) + 4 (klen) + 4 (vlen) = 29
    constexpr size_t HEADER_SIZE = 4;   // CRC32
    constexpr size_t FIXED_PAYLOAD = 8 + 8 + 1 + 4;  // seq + ts + op + klen
    constexpr size_t MIN_SIZE = HEADER_SIZE + FIXED_PAYLOAD + 4;  // + vlen

    if (len < MIN_SIZE) {
        return false;
    }

    uint32_t stored_crc = read_u32(data);
    const uint8_t* payload = data + HEADER_SIZE;
    size_t payload_avail = len - HEADER_SIZE;

    // Read fixed fields
    uint64_t seq_no       = read_u64(payload + 0);
    uint64_t timestamp_ms = read_u64(payload + 8);
    auto     op_type      = static_cast<OpType>(payload[16]);
    uint32_t key_len      = read_u32(payload + 17);

    // Check we can read past the key to the val_len field
    // payload offset of val_len = 21 + key_len
    if (21 + key_len + 4 > payload_avail) {
        return false;
    }

    uint32_t val_len = read_u32(payload + 21 + key_len);

    // Total payload size
    size_t total_payload = 21 + key_len + 4 + val_len;
    size_t total_record  = HEADER_SIZE + total_payload;

    if (total_record > len) {
        return false;
    }

    // Validate CRC32
    uint32_t computed_crc = crc32(payload, total_payload);
    if (computed_crc != stored_crc) {
        return false;
    }

    // Parse key and value
    out.seq_no       = seq_no;
    out.timestamp_ms = timestamp_ms;
    out.op_type      = op_type;
    out.key.assign(reinterpret_cast<const char*>(payload + 21),              key_len);
    out.value.assign(reinterpret_cast<const char*>(payload + 25 + key_len),  val_len);

    bytes_consumed = total_record;
    return true;
}

}  // namespace dkv
