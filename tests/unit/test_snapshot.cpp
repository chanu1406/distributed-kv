#include <gtest/gtest.h>

#include "storage/snapshot.h"
#include "storage/storage_engine.h"
#include "storage/wal.h"

#include <filesystem>

namespace {

class SnapshotTest : public ::testing::Test {
protected:
    std::string test_dir;

    void SetUp() override {
        test_dir = "/tmp/dkv_snap_test_" + std::to_string(::getpid()) + "_" +
                   std::to_string(counter_++);
        std::filesystem::remove_all(test_dir);
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir);
    }

private:
    static int counter_;
};

int SnapshotTest::counter_ = 0;

}  // namespace

TEST_F(SnapshotTest, SaveAndLoad) {
    dkv::StorageEngine engine;
    engine.set("key1", "value1", {100, 1});
    engine.set("key2", "value2", {200, 2});
    engine.del("key3", {300, 1});  // tombstone

    ASSERT_TRUE(dkv::Snapshot::save(engine, 42, test_dir));

    auto data = dkv::Snapshot::load(test_dir + "/snapshot_42.dat");
    ASSERT_TRUE(data.has_value());
    EXPECT_EQ(data->seq_no, 42u);
    EXPECT_EQ(data->entries.size(), 3u);

    // Build a lookup for verification (order from all_entries is not guaranteed)
    std::unordered_map<std::string, dkv::ValueEntry> loaded;
    for (const auto& [k, v] : data->entries) {
        loaded[k] = v;
    }

    EXPECT_EQ(loaded["key1"].value, "value1");
    EXPECT_FALSE(loaded["key1"].is_tombstone);
    EXPECT_EQ(loaded["key1"].version.timestamp_ms, 100u);

    EXPECT_EQ(loaded["key2"].value, "value2");
    EXPECT_FALSE(loaded["key2"].is_tombstone);

    EXPECT_TRUE(loaded["key3"].is_tombstone);
    EXPECT_EQ(loaded["key3"].version.timestamp_ms, 300u);
}

TEST_F(SnapshotTest, FindLatest) {
    dkv::StorageEngine engine;
    engine.set("k", "v", {100, 1});

    // Create multiple snapshots
    dkv::Snapshot::save(engine, 10, test_dir);
    dkv::Snapshot::save(engine, 50, test_dir);
    dkv::Snapshot::save(engine, 30, test_dir);

    auto latest = dkv::Snapshot::find_latest(test_dir);
    ASSERT_TRUE(latest.has_value());

    // Load it and verify it's the seq 50 snapshot
    auto data = dkv::Snapshot::load(*latest);
    ASSERT_TRUE(data.has_value());
    EXPECT_EQ(data->seq_no, 50u);
}

TEST_F(SnapshotTest, FullRecoveryIntegration) {
    // Phase 1: Build state, save snapshot at seq 5, then append more WAL entries
    {
        dkv::StorageEngine engine;
        engine.set("key1", "v1", {100, 1});
        engine.set("key2", "v2", {200, 1});

        // Save snapshot at seq 5
        ASSERT_TRUE(dkv::Snapshot::save(engine, 5, test_dir));

        // Append more WAL entries after the snapshot
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        // seq 1-5 would have been before the snapshot (already captured)
        // We simulate appending new ops with seq 6+
        {
            dkv::WalRecord rec;
            rec.timestamp_ms = 300;
            rec.op_type      = dkv::OpType::SET;
            rec.key           = "key3";
            rec.value         = "v3";
            wal.append(rec);  // seq 1 in WAL file
        }
        {
            dkv::WalRecord rec;
            rec.timestamp_ms = 400;
            rec.op_type      = dkv::OpType::SET;
            rec.key           = "key1";
            rec.value         = "v1_updated";
            wal.append(rec);  // seq 2 in WAL file
        }
        wal.sync();
        wal.close();
    }

    // Phase 2: Recover from snapshot + WAL
    {
        // Load snapshot
        auto snap_path = dkv::Snapshot::find_latest(test_dir);
        ASSERT_TRUE(snap_path.has_value());

        auto snap_data = dkv::Snapshot::load(*snap_path);
        ASSERT_TRUE(snap_data.has_value());
        EXPECT_EQ(snap_data->seq_no, 5u);

        // Rebuild engine from snapshot
        dkv::StorageEngine engine;
        for (const auto& [key, entry] : snap_data->entries) {
            if (entry.is_tombstone) {
                engine.del(key, entry.version);
            } else {
                engine.set(key, entry.value, entry.version);
            }
        }

        // Replay WAL entries that came after the snapshot
        dkv::WAL wal;
        ASSERT_TRUE(wal.open(test_dir));
        auto wal_records = wal.recover();

        for (const auto& rec : wal_records) {
            // In a real system we'd filter by seq_no > snapshot_seq
            // Here we apply all since the WAL only has post-snapshot ops
            if (rec.op_type == dkv::OpType::SET) {
                engine.set(rec.key, rec.value, {rec.timestamp_ms, 1});
            } else {
                engine.del(rec.key, {rec.timestamp_ms, 1});
            }
        }
        wal.close();

        // Verify final state
        auto r1 = engine.get("key1");
        EXPECT_TRUE(r1.found);
        EXPECT_EQ(r1.value, "v1_updated");  // updated by WAL replay

        auto r2 = engine.get("key2");
        EXPECT_TRUE(r2.found);
        EXPECT_EQ(r2.value, "v2");  // from snapshot

        auto r3 = engine.get("key3");
        EXPECT_TRUE(r3.found);
        EXPECT_EQ(r3.value, "v3");  // from WAL replay
    }
}
