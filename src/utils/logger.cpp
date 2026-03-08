#include "utils/logger.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>

namespace dkv {

const char* log_level_str(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO ";
        case LogLevel::WARN:  return "WARN ";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
    }
    return "?????";
}

LogLevel parse_log_level(const std::string& s) {
    if (s == "DEBUG") return LogLevel::DEBUG;
    if (s == "WARN")  return LogLevel::WARN;
    if (s == "ERROR") return LogLevel::ERROR;
    if (s == "FATAL") return LogLevel::FATAL;
    return LogLevel::INFO;
}

// ── Logger ────────────────────────────────────────────────────────────────────

Logger::Logger() : out_(&std::cerr) {}

Logger& Logger::instance() {
    static Logger inst;
    return inst;
}

void Logger::set_level(LogLevel level) {
    std::lock_guard<std::mutex> lk(mu_);
    min_level_ = level;
}

void Logger::set_stream(std::ostream* out) {
    std::lock_guard<std::mutex> lk(mu_);
    out_ = out ? out : &std::cerr;
}

bool Logger::is_enabled(LogLevel level) const {
    return static_cast<int>(level) >= static_cast<int>(min_level_);
}

void Logger::log(LogLevel level, const char* /*file*/, int /*line*/,
                 const std::string& msg) {
    // Build timestamp: HH:MM:SS.mmm
    using namespace std::chrono;
    auto now      = system_clock::now();
    auto now_ms   = duration_cast<milliseconds>(now.time_since_epoch());
    std::time_t t = system_clock::to_time_t(now);
    std::tm tm_buf{};
#ifdef _WIN32
    localtime_s(&tm_buf, &t);
#else
    localtime_r(&t, &tm_buf);
#endif
    int ms = static_cast<int>(now_ms.count() % 1000);

    std::lock_guard<std::mutex> lk(mu_);
    if (static_cast<int>(level) < static_cast<int>(min_level_)) return;

    char tbuf[16];
    std::snprintf(tbuf, sizeof(tbuf), "%02d:%02d:%02d.%03d",
                  tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec, ms);

    *out_ << "[" << tbuf << "] [" << log_level_str(level) << "] " << msg << "\n";
    out_->flush();
}

}  // namespace dkv
