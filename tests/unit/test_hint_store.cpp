#include <gtest/gtest.h>

#include "replication/hint_store.h"
#include "storage/storage_engine.h"

#include <chrono>
#include <filesystem>
#include <string>

// ---------------------------------------------------------------------------
// HintStore unit tests: in-memory operations + disk persistence / recovery
// ---------------------------------------------------------------------------

namespace {

dkv::Hint make_hint(uint32_t target_node_id,
                    const std::string& target_address,
                    const std::string& key,
                    const std::string& value,
                    bool is_del,
                    uint64_t ts_ms,
                    uint32_t origin_node_id) {
    dkv::Hint h;
    h.target_node_id = target_node_id;
    h.target_address = target_address;
    h.key            = key;
    h.value          = value;
    h.is_del         = is_del;
    h.version        = dkv::Version{ts_ms, origin_node_id};
    return h;
}

/// RAII temp-directory: created on construction, removed on destruction.
struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix) {
        auto tick = std::chrono::steady_clock::now().time_since_epoch().count();
        path = std::filesystem::temp_directory_path().string()
             + "/" + prefix + "_" + std::to_string(tick);
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

}  // namespace

// ── In-memory operations ─────────────────────────────────────────────────────

TEST(HintStore, StoreAndRetrieve) {
    dkv::HintStore store;   // no hints_dir → in-memory only

    store.store(make_hint(2, "127.0.0.1:7002", "mykey", "myval",
                          false, 1000, 1));

    auto hints = store.get_hints_for(2);
    ASSERT_EQ(hints.size(), 1u);
    EXPECT_EQ(hints[0].key,   "mykey");
    EXPECT_EQ(hints[0].value, "myval");
    EXPECT_FALSE(hints[0].is_del);
    EXPECT_EQ(hints[0].version.timestamp_ms, 1000ULL);
    EXPECT_EQ(hints[0].version.node_id,      1u);
    EXPECT_EQ(hints[0].target_address, "127.0.0.1:7002");
}

TEST(HintStore, MultipleHintsForSameNode) {
    dkv::HintStore store;

    store.store(make_hint(3, "h:7003", "k1", "v1", false, 100, 1));
    store.store(make_hint(3, "h:7003", "k2", "v2", false, 200, 1));
    store.store(make_hint(3, "h:7003", "k3", "",   true,  300, 1));

    EXPECT_EQ(store.get_hints_for(3).size(), 3u);
}

TEST(HintStore, HintsForDifferentNodes) {
    dkv::HintStore store;

    store.store(make_hint(2, "h:7002", "a", "va", false, 1, 1));
    store.store(make_hint(3, "h:7003", "b", "vb", false, 2, 1));
    store.store(make_hint(2, "h:7002", "c", "vc", false, 3, 1));

    EXPECT_EQ(store.get_hints_for(2).size(),  2u);
    EXPECT_EQ(store.get_hints_for(3).size(),  1u);
    EXPECT_EQ(store.get_hints_for(99).size(), 0u);
}

TEST(HintStore, ClearHintsForNode) {
    dkv::HintStore store;

    store.store(make_hint(2, "h:7002", "k", "v", false, 1, 1));
    store.store(make_hint(3, "h:7003", "k", "v", false, 2, 1));

    store.clear_hints_for(2);

    EXPECT_EQ(store.get_hints_for(2).size(), 0u);
    EXPECT_EQ(store.get_hints_for(3).size(), 1u);  // node 3 unaffected
}

TEST(HintStore, SizeReflectsTotalHints) {
    dkv::HintStore store;

    EXPECT_EQ(store.size(), 0u);

    store.store(make_hint(2, "h:7002", "k1", "v1", false, 1, 1));
    EXPECT_EQ(store.size(), 1u);

    store.store(make_hint(3, "h:7003", "k2", "v2", false, 2, 1));
    store.store(make_hint(3, "h:7003", "k3", "v3", false, 3, 1));
    EXPECT_EQ(store.size(), 3u);

    store.clear_hints_for(3);
    EXPECT_EQ(store.size(), 1u);
}

TEST(HintStore, EmptyStoreReturnsEmpty) {
    dkv::HintStore store;
    EXPECT_EQ(store.get_hints_for(42).size(), 0u);
    EXPECT_EQ(store.size(), 0u);
}

TEST(HintStore, InMemoryModeNoDiskIO) {
    // No hints_dir → load() is a no-op, store() doesn't write files
    dkv::HintStore store;
    store.store(make_hint(2, "h:7002", "k", "v", false, 1, 1));
    store.load();           // must not crash
    EXPECT_EQ(store.size(), 1u);
}

// ── Disk persistence and recovery ────────────────────────────────────────────

TEST(HintStore, PersistAndLoadSingleHint) {
    TempDir tmp("hint_single");

    {
        dkv::HintStore store(tmp.path);
        store.store(make_hint(2, "127.0.0.1:7002", "pkey", "pval",
                              false, 9999, 1));
    }   // store goes out of scope; file flushed

    dkv::HintStore store2(tmp.path);
    store2.load();

    auto hints = store2.get_hints_for(2);
    ASSERT_EQ(hints.size(), 1u);
    EXPECT_EQ(hints[0].key,                  "pkey");
    EXPECT_EQ(hints[0].value,                "pval");
    EXPECT_FALSE(hints[0].is_del);
    EXPECT_EQ(hints[0].version.timestamp_ms, 9999ULL);
    EXPECT_EQ(hints[0].version.node_id,      1u);
    EXPECT_EQ(hints[0].target_node_id,       2u);
    EXPECT_EQ(hints[0].target_address,       "127.0.0.1:7002");
}

TEST(HintStore, PersistMultipleHints) {
    TempDir tmp("hint_multi");

    {
        dkv::HintStore store(tmp.path);
        store.store(make_hint(2, "h:7002", "k1", "v1", false, 100, 1));
        store.store(make_hint(2, "h:7002", "k2", "v2", false, 200, 1));
        store.store(make_hint(3, "h:7003", "k3", "v3", false, 300, 1));
    }

    dkv::HintStore store2(tmp.path);
    store2.load();

    EXPECT_EQ(store2.get_hints_for(2).size(), 2u);
    EXPECT_EQ(store2.get_hints_for(3).size(), 1u);
    EXPECT_EQ(store2.size(), 3u);
}

TEST(HintStore, PersistDelHint) {
    TempDir tmp("hint_del");

    {
        dkv::HintStore store(tmp.path);
        store.store(make_hint(5, "h:7005", "dkey", "", true, 500, 2));
    }

    dkv::HintStore store2(tmp.path);
    store2.load();

    auto hints = store2.get_hints_for(5);
    ASSERT_EQ(hints.size(), 1u);
    EXPECT_EQ(hints[0].key, "dkey");
    EXPECT_TRUE(hints[0].is_del);
    EXPECT_EQ(hints[0].version.timestamp_ms, 500ULL);
}

TEST(HintStore, ClearRemovesDiskFile) {
    TempDir tmp("hint_clear");

    {
        dkv::HintStore store(tmp.path);
        store.store(make_hint(4, "h:7004", "k", "v", false, 1, 1));
    }

    // First load: sees the hint
    dkv::HintStore store2(tmp.path);
    store2.load();
    EXPECT_EQ(store2.size(), 1u);

    // Clear removes file on disk
    store2.clear_hints_for(4);
    EXPECT_EQ(store2.size(), 0u);

    // Second load after clear: nothing to recover
    dkv::HintStore store3(tmp.path);
    store3.load();
    EXPECT_EQ(store3.size(), 0u);
}

TEST(HintStore, LoadOnEmptyDirIsNoop) {
    TempDir tmp("hint_empty");
    dkv::HintStore store(tmp.path);
    store.load();   // must not crash
    EXPECT_EQ(store.size(), 0u);
}

// ── All hint fields survive a disk round-trip ─────────────────────────────────

TEST(HintStore, HintFieldsRoundTrip) {
    TempDir tmp("hint_roundtrip");

    const std::string key   = "round trip key";
    const std::string value = "round trip value with\ttabs";
    const uint64_t    ts    = 1700000099123ULL;
    const uint32_t    nid   = 77;
    const uint32_t    tnode = 9;
    const std::string taddr = "192.168.1.100:9001";

    {
        dkv::HintStore store(tmp.path);
        store.store(make_hint(tnode, taddr, key, value, false, ts, nid));
    }

    dkv::HintStore store2(tmp.path);
    store2.load();

    auto hints = store2.get_hints_for(tnode);
    ASSERT_EQ(hints.size(), 1u);
    EXPECT_EQ(hints[0].key,                  key);
    EXPECT_EQ(hints[0].value,                value);
    EXPECT_EQ(hints[0].target_node_id,       tnode);
    EXPECT_EQ(hints[0].target_address,       taddr);
    EXPECT_EQ(hints[0].version.timestamp_ms, ts);
    EXPECT_EQ(hints[0].version.node_id,      nid);
    EXPECT_FALSE(hints[0].is_del);
}
