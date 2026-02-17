#pragma once

#include <cstdint>
#include <string>

namespace dkv {

struct MurmurHash3Result {
    uint64_t h1;
    uint64_t h2;
};

/// MurmurHash3_x64_128 over raw bytes.
MurmurHash3Result murmurhash3_x64_128(const void* key, size_t len, uint32_t seed = 0);

/// Hash a string and return the primary 64-bit hash.
uint64_t murmurhash3(const std::string& key, uint32_t seed = 0);

}  // namespace dkv
