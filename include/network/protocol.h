#pragma once

#include <cstdint>
#include <string>
#include <optional>

namespace dkv {

/// Command types that can be parsed from the wire protocol.
enum class CommandType : uint8_t {
    SET,
    GET,
    DEL,
    PING,
    FWD,        // Internal forwarded request
};

/// A parsed client request.
struct Command {
    CommandType type;
    std::string key;
    std::string value;          // empty for GET/DEL/PING
    uint64_t    timestamp_ms;   // carried with SET/DEL for versioning
    uint32_t    node_id;        // carried with SET/DEL for versioning

    // FWD fields
    uint32_t    hops_remaining = 2;  // TTL for FWD frames (default 2)
    std::string inner_line;          // opaque inner command (FWD only)
};

/// Result of attempting to parse one command from a byte buffer.
enum class ParseStatus {
    OK,             // a complete command was parsed
    INCOMPLETE,     // need more data (no \n found yet)
    ERROR,          // malformed frame
};

struct ParseResult {
    ParseStatus status;
    Command     command;        // valid only when status == OK
    size_t      bytes_consumed; // how many bytes of the buffer this frame used
    std::string error_msg;      // human-readable, set when status == ERROR
};

/// Try to parse a single command from `buffer`.
/// On success, `bytes_consumed` indicates how many bytes were used, so the
/// caller can advance its read cursor.
///
/// Wire format (newline-terminated, inline length fields):
///   SET <key_len> <key> <val_len> <value>\n
///   GET <key_len> <key>\n
///   DEL <key_len> <key>\n
///   PING\n
///   FWD <hops_remaining> <inner_command_without_newline>\n
ParseResult try_parse(const char* data, size_t len);

// ── Response formatters ─────────────────────────────────────────────────────

/// +OK\n
std::string format_ok();

/// $<val_len> <value>\n
std::string format_value(const std::string& value);

/// -ERR <message>\n
std::string format_error(const std::string& message);

/// -NOT_FOUND\n
std::string format_not_found();

/// +PONG\n
std::string format_pong();

/// FWD <hops> <inner_command>\n
/// Wraps an existing command line for inter-node forwarding.
std::string format_forward(uint32_t hops, const std::string& inner_line);

}  // namespace dkv
