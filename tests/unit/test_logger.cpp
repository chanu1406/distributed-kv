#include "utils/logger.h"

#include <gtest/gtest.h>
#include <sstream>
#include <thread>
#include <vector>

using namespace dkv;

class LoggerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Reset logger to INFO level, redirect to our stream
        Logger::instance().set_level(LogLevel::INFO);
        Logger::instance().set_stream(&buf_);
        buf_.str("");
    }

    void TearDown() override {
        // Restore to stderr, INFO level
        Logger::instance().set_level(LogLevel::INFO);
        Logger::instance().set_stream(nullptr);
    }

    std::ostringstream buf_;
};

TEST_F(LoggerTest, InfoMessageAppears) {
    LOG_INFO("hello world");
    EXPECT_NE(buf_.str().find("hello world"), std::string::npos);
    EXPECT_NE(buf_.str().find("INFO"), std::string::npos);
}

TEST_F(LoggerTest, DebugSuppressedAtInfoLevel) {
    LOG_DEBUG("secret debug");
    EXPECT_TRUE(buf_.str().empty());
}

TEST_F(LoggerTest, WarnAppearsAtInfoLevel) {
    LOG_WARN("some warning");
    EXPECT_NE(buf_.str().find("WARN"), std::string::npos);
}

TEST_F(LoggerTest, ErrorAppearsAtInfoLevel) {
    LOG_ERROR("some error");
    EXPECT_NE(buf_.str().find("ERROR"), std::string::npos);
}

TEST_F(LoggerTest, DebugAppearsWhenLevelSetToDebug) {
    Logger::instance().set_level(LogLevel::DEBUG);
    LOG_DEBUG("debug msg");
    EXPECT_NE(buf_.str().find("debug msg"), std::string::npos);
    EXPECT_NE(buf_.str().find("DEBUG"), std::string::npos);
}

TEST_F(LoggerTest, AllSuppressedAtFatalLevel) {
    Logger::instance().set_level(LogLevel::FATAL);
    LOG_DEBUG("d");
    LOG_INFO("i");
    LOG_WARN("w");
    LOG_ERROR("e");
    EXPECT_TRUE(buf_.str().empty());
}

TEST_F(LoggerTest, FatalAppearsAtFatalLevel) {
    Logger::instance().set_level(LogLevel::FATAL);
    LOG_FATAL("fatal event");
    EXPECT_NE(buf_.str().find("fatal event"), std::string::npos);
}

TEST_F(LoggerTest, ParseLogLevel) {
    EXPECT_EQ(parse_log_level("DEBUG"), LogLevel::DEBUG);
    EXPECT_EQ(parse_log_level("INFO"),  LogLevel::INFO);
    EXPECT_EQ(parse_log_level("WARN"),  LogLevel::WARN);
    EXPECT_EQ(parse_log_level("ERROR"), LogLevel::ERROR);
    EXPECT_EQ(parse_log_level("FATAL"), LogLevel::FATAL);
    EXPECT_EQ(parse_log_level("GARBAGE"), LogLevel::INFO);  // default
}

TEST_F(LoggerTest, ThreadSafety) {
    // Multiple threads logging simultaneously should not crash
    Logger::instance().set_level(LogLevel::DEBUG);
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([i] {
            for (int j = 0; j < 100; ++j) {
                LOG_INFO("thread " << i << " iter " << j);
            }
        });
    }
    for (auto& t : threads) t.join();
    // Just verify it didn't crash and produced output
    EXPECT_FALSE(buf_.str().empty());
}

TEST_F(LoggerTest, MacroStreamConcatenation) {
    int x = 42;
    std::string key = "mykey";
    LOG_INFO("key=" << key << " val=" << x);
    EXPECT_NE(buf_.str().find("key=mykey val=42"), std::string::npos);
}

TEST_F(LoggerTest, TimestampInOutput) {
    LOG_INFO("ts check");
    // Timestamp is in HH:MM:SS.mmm format inside brackets
    const std::string& out = buf_.str();
    EXPECT_EQ(out[0], '[');
    // Should contain colons (time separators)
    EXPECT_NE(out.find(':'), std::string::npos);
}
