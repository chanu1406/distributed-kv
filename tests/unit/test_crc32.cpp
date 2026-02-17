#include <gtest/gtest.h>

#include "utils/crc32.h"

#include <string>

TEST(CRC32, KnownValue) {
    // CRC32 of "123456789" is a well-known test vector: 0xCBF43926
    std::string input = "123456789";
    uint32_t checksum = dkv::crc32(input);
    EXPECT_EQ(checksum, 0xCBF43926u);
}

TEST(CRC32, EmptyInput) {
    uint32_t checksum = dkv::crc32("", 0);
    EXPECT_EQ(checksum, 0x00000000u);
}

TEST(CRC32, Deterministic) {
    std::string data = "some arbitrary payload";
    EXPECT_EQ(dkv::crc32(data), dkv::crc32(data));
}

TEST(CRC32, DifferentInputsDifferentChecksums) {
    EXPECT_NE(dkv::crc32("abc"), dkv::crc32("abd"));
}

TEST(CRC32, StringOverloadMatchesRawOverload) {
    std::string s = "test data";
    EXPECT_EQ(dkv::crc32(s), dkv::crc32(s.data(), s.size()));
}
