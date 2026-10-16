#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <dlfcn.h>
#include <unistd.h>

// Test-only gates at existing locks/queues avoid adding fault hooks to the
// production API. All dependent standard headers precede the access override.
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#include "storage/storage_engine.h"
#undef private
#include "cluster/connection_pool.h"
#include "cluster/hash_ring.h"
#include "cluster/membership.h"
#include "network/protocol.h"
#include "network/thread_pool.h"
#include "replication/hint_store.h"
#include "storage/snapshot.h"
#include "storage/wal.h"
#include <queue>
#define private public
#include "cluster/coordinator.h"
#undef private
#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace {
using namespace std::chrono_literals;
thread_local std::promise<void>* written = nullptr;
thread_local std::shared_future<void> write_gate;
}

// Only this test executable interposes write. A selected writer pauses after
// its real WAL write, before append returns and before the engine is updated.
extern "C" ssize_t write(int fd, const void* bytes, size_t count) {
    using Write = ssize_t (*)(int, const void*, size_t);
    static const auto real_write = reinterpret_cast<Write>(dlsym(RTLD_NEXT, "write"));
    const ssize_t result = real_write(fd, bytes, count);
    if (written) {
        auto* notification = written;
        written = nullptr;
        notification->set_value();
        if (write_gate.valid()) write_gate.wait();
    }
    return result;
}

namespace {
class RecoveryBoundaryTest : public ::testing::Test {
protected:
    std::string directory;
    void SetUp() override {
        std::string pattern = std::filesystem::temp_directory_path().string() + "/dkv-boundary-XXXXXX";
        ASSERT_NE(::mkdtemp(pattern.data()), nullptr);
        directory = pattern;
    }
    void TearDown() override { std::filesystem::remove_all(directory); }
};

dkv::Command put(const std::string& key) {
    dkv::Command command{};
    command.type = dkv::CommandType::RSET;
    command.key = key;
    command.value = "value";
    command.timestamp_ms = 100;
    command.node_id = 2;
    return command;
}
}

TEST_F(RecoveryBoundaryTest, SnapshotCannotDiscardAnUnappliedWalRecord) {
    dkv::StorageEngine engine;
    dkv::HashRing ring;
    dkv::ConnectionPool pool;
    std::string early = "early", late = "late";
    while (engine.shard_index(early) == engine.shard_index(late)) early += "x";
    {
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(directory + "/wal"));
        dkv::Coordinator coordinator(engine, ring, pool, 1, &wal, directory + "/snap", 2);
        ASSERT_EQ(coordinator.handle_command(put("seed")), "+OK\n");
        std::promise<void> early_written, late_written, release_late;
        auto early_done = early_written.get_future();
        auto late_done = late_written.get_future();
        auto gate = release_late.get_future().share();
        std::unique_lock shard_lock(engine.shards_[engine.shard_index(early)].mutex);
        std::thread earlier([&] {
            written = &early_written;
            coordinator.handle_command(put(early));
        });
        early_done.wait();
        std::thread later([&] {
            written = &late_written;
            write_gate = gate;
            coordinator.handle_command(put(late));
        });
        // Original code admits the late append while early awaits its shard.
        // Fixed code blocks it at the mutation lock. Exercise either schedule
        // without deadlocking the regression after the repair.
        const bool overtook = late_done.wait_for(100ms) == std::future_status::ready;
        shard_lock.unlock();
        if (overtook) {
            const auto snapshot = directory + "/snap/snapshot_3.dat";
            const auto deadline = std::chrono::steady_clock::now() + 5s;
            while ((!std::filesystem::exists(snapshot) ||
                    std::filesystem::file_size(snapshot) <= 16) &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(1ms);
            }
        }
        release_late.set_value();
        earlier.join();
        later.join();
        ASSERT_TRUE(engine.get(late).found);
        wal.close(); // also permits the original code's non-RAII WAL in repro mode
    }
    const auto path = dkv::Snapshot::find_latest(directory + "/snap");
    ASSERT_TRUE(path.has_value());
    const auto snapshot = dkv::Snapshot::load(*path);
    ASSERT_TRUE(snapshot.has_value());
    dkv::StorageEngine recovered;
    for (const auto& [key, entry] : snapshot->entries) recovered.set(key, entry.value, entry.version);
    dkv::WAL wal;
    ASSERT_TRUE(wal.open(directory + "/wal"));
#ifdef DKV_ORIGINAL_REPRO
    const auto records = wal.recover();
#else
    const auto records = wal.recover(snapshot->seq_no);
#endif
    for (const auto& record : records) {
        if (record.seq_no > snapshot->seq_no) {
#ifdef DKV_ORIGINAL_REPRO
            recovered.set(record.key, record.value, {record.timestamp_ms, 2});
#else
            recovered.set(record.key, record.value, {record.timestamp_ms, record.node_id.value_or(1)});
#endif
        }
    }
    wal.close();
    EXPECT_TRUE(recovered.get(late).found);
    EXPECT_EQ(recovered.get(late).value, "value");
}

#ifndef DKV_ORIGINAL_REPRO
TEST_F(RecoveryBoundaryTest, QueuedRepairDrainsBeforeWalDestruction) {
    dkv::StorageEngine engine;
    dkv::HashRing ring;
    dkv::ConnectionPool pool;
    std::promise<void> started, release;
    auto gate = release.get_future().share();
    {
        // Same declaration order as main: WAL outlives coordinator.
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(directory, 1, 0));
        dkv::Coordinator coordinator(engine, ring, pool, 1, &wal);
        {
            std::lock_guard lock(coordinator.repair_mutex_);
            coordinator.repair_queue_.emplace([&] { started.set_value(); gate.wait(); });
        }
        coordinator.repair_cv_.notify_one();
        started.get_future().wait();
        coordinator.read_repair_async("key", "value", {100, 2}, false, {{1, "unused"}});
        release.set_value();
    }
    dkv::WAL wal;
    ASSERT_TRUE(wal.open(directory));
    const auto records = wal.recover();
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].key, "key");
    EXPECT_EQ(records[0].value, "value");
    EXPECT_EQ(records[0].node_id, 2u);
}
#endif
