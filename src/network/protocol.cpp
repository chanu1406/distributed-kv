#include "network/protocol.h"

#include <cstring>
#include <charconv>

namespace dkv {

// ── Internal helpers ─────────────────────────────────────────────────────────

namespace {

/// Advance `pos` past any single space at data[pos].  Returns false if
/// there is no space (parse error).
bool consume_space(const char* data, size_t end, size_t& pos) {
    if (pos >= end || data[pos] != ' ') return false;
    ++pos;
    return true;
}

/// Parse an unsigned 32-bit integer starting at data[pos] and ending at the
/// next space or end-of-line.  Advances `pos` past the digits.
bool parse_u32(const char* data, size_t end, size_t& pos, uint32_t& out) {
    if (pos >= end) return false;

    // Find extent of digits
    size_t start = pos;
    while (pos < end && data[pos] >= '0' && data[pos] <= '9') {
        ++pos;
    }
    if (pos == start) return false;  // no digits

    auto [ptr, ec] = std::from_chars(data + start, data + pos, out);
    return ec == std::errc{};
}

/// Read exactly `count` bytes starting at data[pos] into `out`.
/// Advances `pos` by `count`.
bool read_bytes(const char* data, size_t end, size_t& pos,
                size_t count, std::string& out) {
    if (pos + count > end) return false;
    out.assign(data + pos, count);
    pos += count;
    return true;
}

}  // namespace

// ── Parser ───────────────────────────────────────────────────────────────────

ParseResult try_parse(const char* data, size_t len) {
    // Find the first newline – that marks the end of this frame
    const char* nl = static_cast<const char*>(std::memchr(data, '\n', len));
    if (!nl) {
        return {ParseStatus::INCOMPLETE, {}, 0, ""};
    }

    size_t frame_end  = static_cast<size_t>(nl - data);      // index of \n
    size_t total_size = frame_end + 1;                        // include \n
    size_t pos = 0;

    // Macro-like helper: return an ERROR ParseResult
    auto make_error = [&](const char* msg) -> ParseResult {
        return {ParseStatus::ERROR, {}, total_size, msg};
    };

    // Parse the command word by finding the first space (or end-of-frame)
    size_t cmd_end = pos;
    while (cmd_end < frame_end && data[cmd_end] != ' ') {
        ++cmd_end;
    }

    std::string cmd_word(data + pos, cmd_end - pos);
    pos = cmd_end;

    Command cmd{};

    // ── PING ────────────────────────────────────────────────────────────
    if (cmd_word == "PING") {
        if (pos != frame_end) {
            return make_error("PING takes no arguments");
        }
        cmd.type = CommandType::PING;
        return {ParseStatus::OK, cmd, total_size, ""};
    }

    // ── GET / DEL ───────────────────────────────────────────────────────
    if (cmd_word == "GET" || cmd_word == "DEL") {
        cmd.type = (cmd_word == "GET") ? CommandType::GET : CommandType::DEL;

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after command");

        uint32_t key_len = 0;
        if (!parse_u32(data, frame_end, pos, key_len))
            return make_error("invalid key_len");

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after key_len");

        if (!read_bytes(data, frame_end, pos, key_len, cmd.key))
            return make_error("key shorter than key_len");

        if (pos != frame_end)
            return make_error("trailing data after key");

        return {ParseStatus::OK, cmd, total_size, ""};
    }

    // ── SET ─────────────────────────────────────────────────────────────
    if (cmd_word == "SET") {
        cmd.type = CommandType::SET;

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after SET");

        uint32_t key_len = 0;
        if (!parse_u32(data, frame_end, pos, key_len))
            return make_error("invalid key_len");

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after key_len");

        if (!read_bytes(data, frame_end, pos, key_len, cmd.key))
            return make_error("key shorter than key_len");

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after key");

        uint32_t val_len = 0;
        if (!parse_u32(data, frame_end, pos, val_len))
            return make_error("invalid val_len");

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after val_len");

        if (!read_bytes(data, frame_end, pos, val_len, cmd.value))
            return make_error("value shorter than val_len");

        if (pos != frame_end)
            return make_error("trailing data after value");

        return {ParseStatus::OK, cmd, total_size, ""};
    }

    return make_error("unknown command");
}

// ── Response formatters ──────────────────────────────────────────────────────

std::string format_ok() {
    return "+OK\n";
}

std::string format_value(const std::string& value) {
    return "$" + std::to_string(value.size()) + " " + value + "\n";
}

std::string format_error(const std::string& message) {
    return "-ERR " + message + "\n";
}

std::string format_not_found() {
    return "-NOT_FOUND\n";
}

std::string format_pong() {
    return "+PONG\n";
}

}  // namespace dkv
