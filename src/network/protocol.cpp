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

    size_t start = pos;
    while (pos < end && data[pos] >= '0' && data[pos] <= '9') {
        ++pos;
    }
    if (pos == start) return false;  // no digits

    auto [ptr, ec] = std::from_chars(data + start, data + pos, out);
    return ec == std::errc{};
}

/// Parse an unsigned 64-bit integer starting at data[pos].
/// Advances `pos` past the digits.
bool parse_u64(const char* data, size_t end, size_t& pos, uint64_t& out) {
    if (pos >= end) return false;

    size_t start = pos;
    while (pos < end && data[pos] >= '0' && data[pos] <= '9') {
        ++pos;
    }
    if (pos == start) return false;

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

    // ── FWD (internal forwarding) ────────────────────────────────────────
    if (cmd_word == "FWD") {
        cmd.type = CommandType::FWD;

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after FWD");

        uint32_t hops = 0;
        if (!parse_u32(data, frame_end, pos, hops))
            return make_error("invalid hops_remaining");
        cmd.hops_remaining = hops;

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after hops");

        // Store the rest of the line as an opaque inner command string
        if (pos >= frame_end)
            return make_error("missing inner command");
        cmd.inner_line.assign(data + pos, frame_end - pos);

        return {ParseStatus::OK, cmd, total_size, ""};
    }

    // ── RGET (internal versioned GET) ────────────────────────────────────
    // Wire: RGET <key_len> <key>\n  — response: $V ... or -NOT_FOUND\n
    if (cmd_word == "RGET") {
        cmd.type = CommandType::RGET;

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after RGET");

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

    // ── RSET (internal replicated SET with explicit version) ─────────────
    // Wire: RSET <key_len> <key> <val_len> <value> <timestamp_ms> <node_id>\n
    if (cmd_word == "RSET") {
        cmd.type = CommandType::RSET;

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after RSET");

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

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after value");

        if (!parse_u64(data, frame_end, pos, cmd.timestamp_ms))
            return make_error("invalid timestamp_ms");

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after timestamp_ms");

        if (!parse_u32(data, frame_end, pos, cmd.node_id))
            return make_error("invalid node_id");

        if (pos != frame_end)
            return make_error("trailing data after node_id");

        return {ParseStatus::OK, cmd, total_size, ""};
    }

    // ── RDEL (internal replicated DEL with explicit version) ─────────────
    // Wire: RDEL <key_len> <key> <timestamp_ms> <node_id>\n
    if (cmd_word == "RDEL") {
        cmd.type = CommandType::RDEL;

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after RDEL");

        uint32_t key_len = 0;
        if (!parse_u32(data, frame_end, pos, key_len))
            return make_error("invalid key_len");

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after key_len");

        if (!read_bytes(data, frame_end, pos, key_len, cmd.key))
            return make_error("key shorter than key_len");

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after key");

        if (!parse_u64(data, frame_end, pos, cmd.timestamp_ms))
            return make_error("invalid timestamp_ms");

        if (!consume_space(data, frame_end, pos))
            return make_error("expected space after timestamp_ms");

        if (!parse_u32(data, frame_end, pos, cmd.node_id))
            return make_error("invalid node_id");

        if (pos != frame_end)
            return make_error("trailing data after node_id");

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

std::string format_forward(uint32_t hops, const std::string& inner_line) {
    return "FWD " + std::to_string(hops) + " " + inner_line + "\n";
}

std::string format_versioned_value(const std::string& value,
                                   uint64_t timestamp_ms, uint32_t node_id) {
    return "$V " + std::to_string(value.size()) + " " + value + " "
         + std::to_string(timestamp_ms) + " "
         + std::to_string(node_id) + "\n";
}

VersionedGetResult parse_versioned_response(const std::string& resp) {
    VersionedGetResult result;

    // -NOT_FOUND\n
    if (resp == "-NOT_FOUND\n") {
        return result;  // found=false
    }

    // $V <val_len> <value> <timestamp_ms> <node_id>\n
    if (resp.size() < 4 || resp[0] != '$' || resp[1] != 'V' || resp[2] != ' ') {
        return result;  // unrecognised / error response
    }

    // Work on the content between "$V " and the trailing '\n'
    const char* p   = resp.data() + 3;             // after "$V "
    const char* end = resp.data() + resp.size() - 1; // points at '\n'

    if (p >= end) return result;

    // Parse val_len
    uint32_t val_len = 0;
    {
        const char* sp = static_cast<const char*>(std::memchr(p, ' ',
                             static_cast<size_t>(end - p)));
        if (!sp) return result;
        auto [ptr, ec] = std::from_chars(p, sp, val_len);
        if (ec != std::errc{}) return result;
        p = sp + 1;
    }

    // Read val_len bytes of value
    if (p + val_len > end) return result;
    result.value.assign(p, val_len);
    p += val_len;

    // Expect space before timestamp_ms
    if (p >= end || *p != ' ') return result;
    ++p;

    // Parse timestamp_ms (u64)
    {
        const char* sp = static_cast<const char*>(std::memchr(p, ' ',
                             static_cast<size_t>(end - p)));
        if (!sp) return result;
        auto [ptr, ec] = std::from_chars(p, sp, result.timestamp_ms);
        if (ec != std::errc{}) return result;
        p = sp + 1;
    }

    // Parse node_id (u32)
    {
        auto [ptr, ec] = std::from_chars(p, end, result.node_id);
        if (ec != std::errc{}) return result;
    }

    result.found = true;
    return result;
}

}  // namespace dkv
