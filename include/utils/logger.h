#pragma once

#include <cstdint>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>

namespace dkv {

enum class LogLevel : int {
    DEBUG = 0,
    INFO  = 1,
    WARN  = 2,
    ERROR = 3,
    FATAL = 4,
};

// Convert level to string tag
const char* log_level_str(LogLevel level);

// Parse level from string ("DEBUG", "INFO", "WARN", "ERROR", "FATAL").
// Returns INFO if unrecognized.
LogLevel parse_log_level(const std::string& s);

class Logger {
public:
    // Singleton accessor
    static Logger& instance();

    // Configure minimum level and optional output stream (default: std::cerr).
    // Not thread-safe — call once before any logging.
    void set_level(LogLevel level);
    void set_stream(std::ostream* out);  // Does NOT take ownership

    // Log a message at the given level.
    void log(LogLevel level, const char* file, int line, const std::string& msg);

    // Returns true if messages at this level will be emitted.
    bool is_enabled(LogLevel level) const;

private:
    Logger();
    ~Logger() = default;
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    LogLevel    min_level_{LogLevel::INFO};
    std::ostream* out_{nullptr};
    std::mutex  mu_;
};

// ── Logging macros ────────────────────────────────────────────────────────────
// Usage: LOG_INFO("key=" << key << " val=" << val);

#define DKV_LOG(level, msg)                                        \
    do {                                                           \
        auto& _logger = ::dkv::Logger::instance();                 \
        if (_logger.is_enabled(level)) {                           \
            std::ostringstream _oss;                               \
            _oss << msg;                                           \
            _logger.log(level, __FILE__, __LINE__, _oss.str());    \
        }                                                          \
    } while (0)

#define LOG_DEBUG(msg) DKV_LOG(::dkv::LogLevel::DEBUG, msg)
#define LOG_INFO(msg)  DKV_LOG(::dkv::LogLevel::INFO,  msg)
#define LOG_WARN(msg)  DKV_LOG(::dkv::LogLevel::WARN,  msg)
#define LOG_ERROR(msg) DKV_LOG(::dkv::LogLevel::ERROR, msg)
#define LOG_FATAL(msg) DKV_LOG(::dkv::LogLevel::FATAL, msg)

}  // namespace dkv
