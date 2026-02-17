#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace dkv {

/// CRC32 checksum (IEEE 802.3 polynomial).
uint32_t crc32(const void* data, size_t len);
uint32_t crc32(const std::string& data);

}  // namespace dkv
