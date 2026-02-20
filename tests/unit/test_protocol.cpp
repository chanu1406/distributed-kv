#include <gtest/gtest.h>

#include "network/protocol.h"

// ---------------------------------------------------------------------------
// Parsing valid commands
// ---------------------------------------------------------------------------

TEST(Protocol, ParsePing) {
    std::string buf = "PING\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::OK);
    EXPECT_EQ(result.command.type, dkv::CommandType::PING);
    EXPECT_EQ(result.bytes_consumed, buf.size());
}

TEST(Protocol, ParseGet) {
    std::string buf = "GET 5 hello\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::OK);
    EXPECT_EQ(result.command.type, dkv::CommandType::GET);
    EXPECT_EQ(result.command.key, "hello");
    EXPECT_EQ(result.bytes_consumed, buf.size());
}

TEST(Protocol, ParseDel) {
    std::string buf = "DEL 3 foo\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::OK);
    EXPECT_EQ(result.command.type, dkv::CommandType::DEL);
    EXPECT_EQ(result.command.key, "foo");
    EXPECT_EQ(result.bytes_consumed, buf.size());
}

TEST(Protocol, ParseSet) {
    std::string buf = "SET 3 foo 5 hello\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::OK);
    EXPECT_EQ(result.command.type, dkv::CommandType::SET);
    EXPECT_EQ(result.command.key, "foo");
    EXPECT_EQ(result.command.value, "hello");
    EXPECT_EQ(result.bytes_consumed, buf.size());
}

TEST(Protocol, ParseSetWithSpacesInValue) {
    // Value contains spaces — length framing handles this correctly
    std::string buf = "SET 3 key 11 hello world\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::OK);
    EXPECT_EQ(result.command.type, dkv::CommandType::SET);
    EXPECT_EQ(result.command.key, "key");
    EXPECT_EQ(result.command.value, "hello world");
}

TEST(Protocol, ParseSetWithBinaryishValue) {
    // Value contains characters that would break naive parsing
    std::string val = "a\tb\rc";  // tab and carriage return (no newline)
    std::string buf = "SET 4 test " + std::to_string(val.size()) + " " + val + "\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::OK);
    EXPECT_EQ(result.command.value, val);
}

// ---------------------------------------------------------------------------
// Incomplete frames (need more data)
// ---------------------------------------------------------------------------

TEST(Protocol, IncompleteNoNewline) {
    std::string buf = "SET 3 foo 5 hello";  // no trailing \n
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::INCOMPLETE);
    EXPECT_EQ(result.bytes_consumed, 0u);
}

TEST(Protocol, IncompleteEmpty) {
    auto result = dkv::try_parse("", 0);
    EXPECT_EQ(result.status, dkv::ParseStatus::INCOMPLETE);
}

// ---------------------------------------------------------------------------
// Malformed frames (errors)
// ---------------------------------------------------------------------------

TEST(Protocol, ErrorUnknownCommand) {
    std::string buf = "FOOBAR\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::ERROR);
    EXPECT_GT(result.bytes_consumed, 0u);  // frame was consumed
}

TEST(Protocol, ErrorPingWithArgs) {
    std::string buf = "PING extra\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::ERROR);
}

TEST(Protocol, ErrorGetBadKeyLen) {
    std::string buf = "GET abc key\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::ERROR);
}

TEST(Protocol, ErrorSetKeyTooShort) {
    // key_len says 10 but key is only 3 characters before the space
    std::string buf = "SET 10 foo 5 hello\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::ERROR);
}

// ---------------------------------------------------------------------------
// Multiple frames in one buffer
// ---------------------------------------------------------------------------

TEST(Protocol, TwoFramesInBuffer) {
    std::string buf = "PING\nGET 3 foo\n";
    
    // Parse first frame
    auto r1 = dkv::try_parse(buf.data(), buf.size());
    EXPECT_EQ(r1.status, dkv::ParseStatus::OK);
    EXPECT_EQ(r1.command.type, dkv::CommandType::PING);

    // Parse second frame from remaining buffer
    auto r2 = dkv::try_parse(buf.data() + r1.bytes_consumed,
                              buf.size() - r1.bytes_consumed);
    EXPECT_EQ(r2.status, dkv::ParseStatus::OK);
    EXPECT_EQ(r2.command.type, dkv::CommandType::GET);
    EXPECT_EQ(r2.command.key, "foo");
}

// ---------------------------------------------------------------------------
// Response formatting
// ---------------------------------------------------------------------------

TEST(Protocol, FormatOk) {
    EXPECT_EQ(dkv::format_ok(), "+OK\n");
}

TEST(Protocol, FormatValue) {
    EXPECT_EQ(dkv::format_value("hello"), "$5 hello\n");
}

TEST(Protocol, FormatError) {
    EXPECT_EQ(dkv::format_error("QUORUM_FAILED"), "-ERR QUORUM_FAILED\n");
}

TEST(Protocol, FormatNotFound) {
    EXPECT_EQ(dkv::format_not_found(), "-NOT_FOUND\n");
}

TEST(Protocol, FormatPong) {
    EXPECT_EQ(dkv::format_pong(), "+PONG\n");
}

// ---------------------------------------------------------------------------
// FWD protocol
// ---------------------------------------------------------------------------

TEST(Protocol, ParseFwdWithGetInner) {
    std::string buf = "FWD 2 GET 3 foo\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::OK);
    EXPECT_EQ(result.command.type, dkv::CommandType::FWD);
    EXPECT_EQ(result.command.hops_remaining, 2u);
    EXPECT_EQ(result.command.inner_line, "GET 3 foo");
    EXPECT_EQ(result.bytes_consumed, buf.size());
}

TEST(Protocol, ParseFwdWithSetInner) {
    std::string buf = "FWD 1 SET 3 key 5 value\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::OK);
    EXPECT_EQ(result.command.type, dkv::CommandType::FWD);
    EXPECT_EQ(result.command.hops_remaining, 1u);
    EXPECT_EQ(result.command.inner_line, "SET 3 key 5 value");
}

TEST(Protocol, ParseFwdZeroHops) {
    std::string buf = "FWD 0 GET 3 foo\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::OK);
    EXPECT_EQ(result.command.hops_remaining, 0u);
}

TEST(Protocol, FwdErrorMissingHops) {
    std::string buf = "FWD\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::ERROR);
}

TEST(Protocol, FwdErrorMissingInner) {
    std::string buf = "FWD 2\n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    EXPECT_EQ(result.status, dkv::ParseStatus::ERROR);
}

TEST(Protocol, FwdErrorMissingInnerAfterSpace) {
    std::string buf = "FWD 2 \n";
    auto result = dkv::try_parse(buf.data(), buf.size());

    // Space is consumed, then inner_line is empty (pos >= frame_end)
    EXPECT_EQ(result.status, dkv::ParseStatus::ERROR);
}

TEST(Protocol, FormatForward) {
    std::string result = dkv::format_forward(2, "GET 3 foo");
    EXPECT_EQ(result, "FWD 2 GET 3 foo\n");
}

TEST(Protocol, FormatForwardRoundTrip) {
    // format_forward produces a parseable FWD frame
    std::string frame = dkv::format_forward(3, "SET 3 bar 5 world");
    auto parsed = dkv::try_parse(frame.data(), frame.size());

    EXPECT_EQ(parsed.status, dkv::ParseStatus::OK);
    EXPECT_EQ(parsed.command.type, dkv::CommandType::FWD);
    EXPECT_EQ(parsed.command.hops_remaining, 3u);
    EXPECT_EQ(parsed.command.inner_line, "SET 3 bar 5 world");
}
