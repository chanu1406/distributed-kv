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
};

/// A parsed client request.
struct Command {
    CommandType type;
    std::string key;
    std::string value;          // empty for GET/DEL/PING
    uint64_t    timestamp_ms;   // carried with SET/DEL for versioning
    uint32_t    node_id;        // carried with SET/DEL for versioning
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

}  // namespace dkv
