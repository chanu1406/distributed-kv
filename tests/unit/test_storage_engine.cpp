#include <gtest/gtest.h>

#include "storage/storage_engine.h"

#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// Basic CRUD
// ---------------------------------------------------------------------------

TEST(StorageEngine, SetAndGet) {
    dkv::StorageEngine engine;
    dkv::Version v{100, 1};

    EXPECT_TRUE(engine.set("key1", "value1", v));

    auto result = engine.get("key1");
    EXPECT_TRUE(result.found);
    EXPECT_EQ(result.value, "value1");
    EXPECT_EQ(result.version.timestamp_ms, 100u);
    EXPECT_EQ(result.version.node_id, 1u);
}

TEST(StorageEngine, GetMissingKey) {
    dkv::StorageEngine engine;
    auto result = engine.get("nonexistent");
    EXPECT_FALSE(result.found);
}

TEST(StorageEngine, DeleteWritesTombstone) {
    dkv::StorageEngine engine;
    engine.set("key1", "value1", {100, 1});

    EXPECT_TRUE(engine.del("key1", {200, 1}));

    // GET should return not-found (tombstoned)
    auto result = engine.get("key1");
    EXPECT_FALSE(result.found);

    // But the entry still exists internally (for read repair)
    auto entries = engine.all_entries();
    bool found_tombstone = false;
    for (const auto& [k, v] : entries) {
        if (k == "key1") {
            EXPECT_TRUE(v.is_tombstone);
            EXPECT_EQ(v.version.timestamp_ms, 200u);
            found_tombstone = true;
        }
    }
    EXPECT_TRUE(found_tombstone);
}

// ---------------------------------------------------------------------------
// LWW Conflict Resolution
// ---------------------------------------------------------------------------

TEST(StorageEngine, LWW_NewerTimestampWins) {
    dkv::StorageEngine engine;
    engine.set("key1", "old", {100, 1});
    engine.set("key1", "new", {200, 1});

    auto result = engine.get("key1");
    EXPECT_EQ(result.value, "new");
    EXPECT_EQ(result.version.timestamp_ms, 200u);
}

TEST(StorageEngine, LWW_SameTimestamp_HigherNodeIdWins) {
    dkv::StorageEngine engine;
    engine.set("key1", "node1", {100, 1});
    engine.set("key1", "node5", {100, 5});

    auto result = engine.get("key1");
    EXPECT_EQ(result.value, "node5");
    EXPECT_EQ(result.version.node_id, 5u);
}

TEST(StorageEngine, LWW_OlderTimestampRejected) {
    dkv::StorageEngine engine;
    engine.set("key1", "new", {200, 1});

    // This should be rejected — version 100 is older than 200
    EXPECT_FALSE(engine.set("key1", "old", {100, 1}));

    auto result = engine.get("key1");
    EXPECT_EQ(result.value, "new");
    EXPECT_EQ(result.version.timestamp_ms, 200u);
}

TEST(StorageEngine, LWW_DeleteRejectedByNewerSet) {
    dkv::StorageEngine engine;
    engine.set("key1", "value1", {200, 1});

    // Delete with older version should be rejected
    EXPECT_FALSE(engine.del("key1", {100, 1}));

    auto result = engine.get("key1");
    EXPECT_TRUE(result.found);
    EXPECT_EQ(result.value, "value1");
}

TEST(StorageEngine, SetAfterDeleteResurrects) {
    dkv::StorageEngine engine;
    engine.set("key1", "v1", {100, 1});
    engine.del("key1", {200, 1});

    // SET with even newer version should resurrect the key
    EXPECT_TRUE(engine.set("key1", "v2", {300, 1}));

    auto result = engine.get("key1");
    EXPECT_TRUE(result.found);
    EXPECT_EQ(result.value, "v2");
    EXPECT_EQ(result.version.timestamp_ms, 300u);
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

TEST(StorageEngine, ConcurrentReadWrite) {
    dkv::StorageEngine engine;
    constexpr int NUM_KEYS = 100;
    constexpr int NUM_THREADS = 8;

    // Spawn writer threads
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&engine, t]() {
            for (int i = 0; i < NUM_KEYS; i++) {
                std::string key = "key_" + std::to_string(i);
                dkv::Version v{static_cast<uint64_t>(t * NUM_KEYS + i),
                               static_cast<uint32_t>(t)};
                engine.set(key, "val_" + std::to_string(t), v);
            }
        });
    }

    // Spawn reader threads
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&engine]() {
            for (int i = 0; i < NUM_KEYS; i++) {
                engine.get("key_" + std::to_string(i));
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Verify all keys exist (the highest-versioned write should have won)
    for (int i = 0; i < NUM_KEYS; i++) {
        auto result = engine.get("key_" + std::to_string(i));
        EXPECT_TRUE(result.found);
    }
}
