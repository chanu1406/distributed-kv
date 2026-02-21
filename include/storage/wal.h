#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>
#include <fstream>
#include <mutex>

namespace dkv {

/// Operation types recorded in the WAL.
enum class OpType : uint8_t {
    SET = 0,
    DEL = 1,
};

/// A single WAL record.
struct WalRecord {
    uint64_t seq_no       = 0;
    uint64_t timestamp_ms = 0;
    OpType   op_type      = OpType::SET;
    std::string key;
    std::string value;  // empty for DEL
};

/// Append-only Write-Ahead Log with CRC32 integrity checks.
///
/// Record binary format:
///   [CRC32 4B] [SeqNo 8B] [Timestamp 8B] [OpType 1B]
///   [KeyLen 4B] [Key ...] [ValLen 4B] [Value ...]
///
/// The CRC32 covers everything after the checksum field.
class WAL {
public:
    /// Open (or create) the WAL file at `directory/wal.bin`.
    /// Returns false if the directory cannot be created or the file cannot
    /// be opened.
    bool open(const std::string& directory);

    /// Open with batched fsync parameters.
    /// @param fsync_interval_ms  Max milliseconds between fsyncs (0 = no timer).
    /// @param fsync_batch_ops    Fsync after this many appends (0 = no batching).
    bool open(const std::string& directory,
              uint32_t fsync_interval_ms, uint32_t fsync_batch_ops);

    /// Append a record to the WAL.  Assigns a monotonically increasing
    /// sequence number and returns it.  May trigger an fsync if the
    /// batch-ops threshold is reached.
    uint64_t append(const WalRecord& record);

    /// Read all valid records from the WAL file.  Stops at the first
    /// record with an invalid CRC32 checksum (crash-safe recovery).
    std::vector<WalRecord> recover();

    /// Explicitly fsync the WAL file to disk.
    void sync();

    /// The current (last assigned) sequence number.
    uint64_t current_seq_no() const { return next_seq_no_ - 1; }

    /// Close the WAL file.  Stops the background fsync thread and
    /// performs a final fsync.
    void close();

private:
    std::string   filepath_;
    int           fd_ = -1;      // POSIX file descriptor
    uint64_t      next_seq_no_ = 1;
    std::mutex    mutex_;

    // ── Batched fsync state ──────────────────────────────────────────────
    uint32_t              fsync_interval_ms_ = 0;   // 0 = no background timer
    uint32_t              fsync_batch_ops_   = 0;   // 0 = no ops-based batching
    std::atomic<uint32_t> ops_since_sync_{0};
    std::atomic<bool>     dirty_{false};

    std::thread           fsync_thread_;
    std::atomic<bool>     fsync_running_{false};
    std::mutex            fsync_mutex_;
    std::condition_variable fsync_cv_;

    /// Background fsync thread loop.
    void fsync_loop();

    /// Serialize a record into a byte buffer (including CRC32 header).
    static std::vector<uint8_t> serialize(const WalRecord& record);

    /// Deserialize a record from raw bytes.  Returns false if CRC32
    /// validation fails or the buffer is too short.
    static bool deserialize(const uint8_t* data, size_t len,
                            WalRecord& out, size_t& bytes_consumed);
};

}  // namespace dkv
