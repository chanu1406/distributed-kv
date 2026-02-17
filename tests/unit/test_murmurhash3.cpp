#include <gtest/gtest.h>

#include "utils/murmurhash3.h"

#include <set>
#include <string>

TEST(MurmurHash3, DeterministicOutput) {
    auto h1 = dkv::murmurhash3("hello");
    auto h2 = dkv::murmurhash3("hello");
    EXPECT_EQ(h1, h2);
}

TEST(MurmurHash3, DifferentKeysProduceDifferentHashes) {
    auto h1 = dkv::murmurhash3("key1");
    auto h2 = dkv::murmurhash3("key2");
    EXPECT_NE(h1, h2);
}

TEST(MurmurHash3, EmptyString) {
    // should not crash, and should produce a valid hash
    auto h = dkv::murmurhash3("");
    (void)h;
}

TEST(MurmurHash3, SeedChangesOutput) {
    auto h1 = dkv::murmurhash3("test", 0);
    auto h2 = dkv::murmurhash3("test", 42);
    EXPECT_NE(h1, h2);
}

TEST(MurmurHash3, Distribution) {
    // hash 1000 sequential keys, verify no collisions
    std::set<uint64_t> hashes;
    for (int i = 0; i < 1000; i++) {
        hashes.insert(dkv::murmurhash3("key_" + std::to_string(i)));
    }
    EXPECT_EQ(hashes.size(), 1000u);
}

TEST(MurmurHash3, FullResult128Bit) {
    auto result = dkv::murmurhash3_x64_128("hello", 5, 0);
    EXPECT_NE(result.h1, 0u);
    EXPECT_NE(result.h2, 0u);
    EXPECT_NE(result.h1, result.h2);
}
