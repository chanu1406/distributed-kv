#include "utils/crc32.h"

#include <array>

namespace dkv {

namespace {

// Generate the CRC32 table at compile time (IEEE 802.3 polynomial 0xEDB88320).
// This eliminates any chance of copy-paste errors from a hardcoded table.
constexpr std::array<uint32_t, 256> generate_crc32_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            if (crc & 1)
                crc = (crc >> 1) ^ 0xEDB88320u;
            else
                crc >>= 1;
        }
        table[i] = crc;
    }
    return table;
}

constexpr auto CRC32_TABLE = generate_crc32_table();

}  // namespace

uint32_t crc32(const void* data, size_t len) {
    const auto* buf = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;

    for (size_t i = 0; i < len; i++) {
        crc = CRC32_TABLE[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    }

    return crc ^ 0xFFFFFFFF;
}

uint32_t crc32(const std::string& data) {
    return crc32(data.data(), data.size());
}

}  // namespace dkv
