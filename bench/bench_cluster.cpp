// Closed-loop TCP benchmark. One outstanding request per connection.
// Preload and per-client warmup are checked and excluded from measured time.
// JSON output is consumed by scripts/run_benchmarks.py.
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace std::chrono;
using BenchClock = steady_clock;

struct Stats {
    std::vector<int64_t> latencies_ns;
    void record(nanoseconds ns) { latencies_ns.push_back(ns.count()); }
    void merge(const Stats& other) {
        latencies_ns.insert(latencies_ns.end(), other.latencies_ns.begin(), other.latencies_ns.end());
    }
    // Nearest-rank quantile, including fractional percentiles.
    double percentile(double p) {
        if (latencies_ns.empty()) return 0;
        std::sort(latencies_ns.begin(), latencies_ns.end());
        const auto rank = static_cast<size_t>(std::ceil(p * latencies_ns.size() / 100.0));
        return latencies_ns[std::clamp(rank, size_t{1}, latencies_ns.size()) - 1] / 1000.0;
    }
};

struct BenchConfig {
    std::string host = "127.0.0.1";
    int port = 7001;
    int ops = 100000;
    int threads = 1;
    int keys = 10000;
    int key_size = 16;
    int val_size = 64;
    int warmup_ops = 1000;  // per client
    double duration_seconds = 0;  // positive value overrides --ops
    std::string workload = "set";
    bool json = false;
};

struct Request {
    std::string set_frame;
    std::string get_frame;
    std::string expected_value;
};

enum class Outcome { success, server_error, protocol_error, transport_error };

struct ThreadState {
    Stats stats;
    uint64_t attempted = 0, successful = 0;
    uint64_t server_errors = 0, protocol_errors = 0, transport_errors = 0;
    uint64_t setup_errors = 0;
};

struct BenchResult : ThreadState {
    double elapsed_seconds = 0;
    double success_ops_per_sec = 0;
    double p50_us = 0, p95_us = 0, p99_us = 0, p999_us = 0;
    bool valid = false;
};

static int open_connection(const BenchConfig& cfg) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    auto fail = [&]() { ::close(fd); return -1; };
    int one = 1;
    timeval timeout{2, 0};
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0 ||
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) return fail();
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(cfg.port));
    if (::inet_pton(AF_INET, cfg.host.c_str(), &addr.sin_addr) != 1) return fail();
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return fail();
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) return fail();
        pollfd descriptor{fd, POLLOUT, 0};
        int ready;
        do { ready = ::poll(&descriptor, 1, 2000); } while (ready < 0 && errno == EINTR);
        int error = 0;
        socklen_t length = sizeof(error);
        if (ready <= 0 || ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) != 0 ||
            error != 0) return fail();
    }
    if (::fcntl(fd, F_SETFL, flags) < 0) return fail();
    return fd;
}

static bool send_all(int fd, const std::string& frame) {
    size_t sent = 0;
    while (sent < frame.size()) {
        const ssize_t n = ::send(fd, frame.data() + sent, frame.size() - sent, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static Outcome exchange(int fd, const Request& request, bool is_set) {
    if (!send_all(fd, is_set ? request.set_frame : request.get_frame))
        return Outcome::transport_error;
    std::string response;
    char buffer[16384];
    const size_t limit = std::max(size_t{1024}, request.expected_value.size());
    while (true) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return Outcome::transport_error;
        response.append(buffer, static_cast<size_t>(n));
        if (response.size() > limit) return Outcome::protocol_error;
        const auto newline = response.find('\n');
        if (newline == std::string::npos) continue;
        // Extra frames cannot belong to another request: pipelining is disabled.
        if (newline + 1 != response.size()) return Outcome::protocol_error;
        if (response.front() == '-') return Outcome::server_error;
        return response == (is_set ? "+OK\n" : request.expected_value)
            ? Outcome::success : Outcome::protocol_error;
    }
}

static std::vector<Request> make_requests(const BenchConfig& cfg) {
    std::vector<Request> requests;
    requests.reserve(static_cast<size_t>(cfg.keys));
    for (int i = 0; i < cfg.keys; ++i) {
        std::string key = "key:" + std::to_string(i);
        key.resize(static_cast<size_t>(cfg.key_size), 'x');
        // Values differ by key so returning another key's value is detectable.
        std::string value = std::to_string(i);
        value.resize(static_cast<size_t>(cfg.val_size), 'v');
        requests.push_back({
            "SET " + std::to_string(key.size()) + " " + key + " " +
                std::to_string(value.size()) + " " + value + "\n",
            "GET " + std::to_string(key.size()) + " " + key + "\n",
            "$" + std::to_string(value.size()) + " " + value + "\n"
        });
    }
    return requests;
}

static BenchResult run_benchmark(const BenchConfig& cfg) {
    BenchResult result;
    const auto requests = make_requests(cfg);
    // All workloads use the same preloaded keyspace: SET measures updates.
    int preload_fd = open_connection(cfg);
    if (preload_fd < 0) { result.setup_errors = 1; return result; }
    for (const auto& request : requests) {
        if (exchange(preload_fd, request, true) != Outcome::success) {
            ::close(preload_fd);
            result.setup_errors = 1;
            return result;
        }
    }
    ::close(preload_fd);

    std::vector<ThreadState> states(static_cast<size_t>(cfg.threads));
    std::vector<std::thread> threads;
    std::mutex mutex;
    std::condition_variable ready_cv, go_cv;
    int ready = 0;
    bool go = false, abort = false;
    BenchClock::time_point deadline;
    for (int t = 0; t < cfg.threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& state = states[static_cast<size_t>(t)];
            const int fd = open_connection(cfg);
            if (fd < 0) ++state.setup_errors;
            auto execute = [&](uint64_t index) {
                const auto key_index = (static_cast<uint64_t>(t) + index * cfg.threads) % requests.size();
                const bool is_set = cfg.workload == "set" || (cfg.workload == "mixed" && index % 2 == 0);
                return exchange(fd, requests[key_index], is_set);
            };
            if (fd >= 0) {
                for (int i = 0; i < cfg.warmup_ops; ++i) {
                    if (execute(static_cast<uint64_t>(i)) != Outcome::success) {
                        ++state.setup_errors;
                        break;
                    }
                }
            }
            {
                std::unique_lock lock(mutex);
                abort = abort || state.setup_errors != 0;
                ++ready;
                ready_cv.notify_one();
                go_cv.wait(lock, [&]() { return go; });
                if (abort) { if (fd >= 0) ::close(fd); return; }
            }
            // Distribute the remainder, so --ops is never silently rounded down.
            const uint64_t count = static_cast<uint64_t>(cfg.ops / cfg.threads +
                                                        (t < cfg.ops % cfg.threads));
            for (uint64_t i = 0; ; ++i) {
                if (cfg.duration_seconds > 0 ? BenchClock::now() >= deadline : i >= count) break;
                ++state.attempted;
                const auto start = BenchClock::now();
                const Outcome outcome = execute(i);
                const auto elapsed = duration_cast<nanoseconds>(BenchClock::now() - start);
                if (outcome == Outcome::success) {
                    ++state.successful;
                    state.stats.record(elapsed);
                } else if (outcome == Outcome::server_error) {
                    ++state.server_errors;
                } else {
                    if (outcome == Outcome::transport_error) ++state.transport_errors;
                    else ++state.protocol_errors;
                    // Never reuse a stream after a timeout, truncation, or malformed reply.
                    break;
                }
            }
            ::close(fd);
        });
    }

    BenchClock::time_point start;
    {
        std::unique_lock lock(mutex);
        ready_cv.wait(lock, [&]() { return ready == cfg.threads; });
        start = BenchClock::now();
        deadline = start + duration_cast<BenchClock::duration>(duration<double>(cfg.duration_seconds));
        go = true;
        go_cv.notify_all();
    }
    for (auto& thread : threads) thread.join();
    result.elapsed_seconds = duration<double>(BenchClock::now() - start).count();
    for (const auto& state : states) {
        result.stats.merge(state.stats);
        result.attempted += state.attempted;
        result.successful += state.successful;
        result.server_errors += state.server_errors;
        result.protocol_errors += state.protocol_errors;
        result.transport_errors += state.transport_errors;
        result.setup_errors += state.setup_errors;
    }
    if (result.elapsed_seconds > 0)
        result.success_ops_per_sec = result.successful / result.elapsed_seconds;
    result.p50_us = result.stats.percentile(50);
    result.p95_us = result.stats.percentile(95);
    result.p99_us = result.stats.percentile(99);
    result.p999_us = result.stats.percentile(99.9);
    result.valid = result.setup_errors == 0 && result.attempted == result.successful &&
        result.successful > 0 && (cfg.duration_seconds > 0 || result.attempted == static_cast<uint64_t>(cfg.ops));
    return result;
}

static void print_json(const BenchConfig& cfg, const BenchResult& result) {
    std::cout << std::setprecision(12)
        << "{\"schema_version\":1,\"valid\":" << (result.valid ? "true" : "false")
        << ",\"workload\":\"" << cfg.workload << "\",\"threads\":" << cfg.threads
        << ",\"keys\":" << cfg.keys << ",\"key_size\":" << cfg.key_size
        << ",\"val_size\":" << cfg.val_size << ",\"pipeline\":1"
        << ",\"warmup_ops_per_client\":" << cfg.warmup_ops
        << ",\"requested_ops\":" << cfg.ops << ",\"requested_duration_seconds\":" << cfg.duration_seconds
        << ",\"attempted\":" << result.attempted << ",\"successful\":" << result.successful
        << ",\"server_errors\":" << result.server_errors << ",\"protocol_errors\":" << result.protocol_errors
        << ",\"transport_errors\":" << result.transport_errors << ",\"setup_errors\":" << result.setup_errors
        << ",\"errors\":" << result.server_errors + result.protocol_errors + result.transport_errors
        << ",\"latency_samples\":" << result.stats.latencies_ns.size()
        << ",\"elapsed_seconds\":" << result.elapsed_seconds
        << ",\"success_ops_per_sec\":" << result.success_ops_per_sec
        << ",\"p50_us\":" << result.p50_us << ",\"p95_us\":" << result.p95_us
        << ",\"p99_us\":" << result.p99_us << ",\"p999_us\":" << result.p999_us << "}\n";
}

static int parse_int(const std::string& text) {
    size_t end = 0;
    const int result = std::stoi(text, &end);
    if (end != text.size()) throw std::invalid_argument("invalid integer: " + text);
    return result;
}

int main(int argc, char* argv[]) {
    std::signal(SIGPIPE, SIG_IGN);
    BenchConfig cfg;
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string flag = argv[i];
            if (flag == "--json") { cfg.json = true; continue; }
            if (flag == "--help") {
                std::cout << "bench_cluster [--host IPv4] [--port N] [--workload set|get|mixed|readonly]\n"
                    "  [--ops N | --duration-seconds S] [--threads N] [--keys N]\n"
                    "  [--key-size N] [--val-size N] [--warmup-ops N] [--pipeline 1] [--json]\n"
                    "Preloads a fixed keyspace. SET updates existing keys. Warmup is per client.\n"
                    "Latency quantiles cover successful requests; throughput excludes errors.\n";
                return 0;
            }
            if (i + 1 == argc) throw std::invalid_argument("missing value for " + flag);
            const std::string value = argv[++i];
            if (flag == "--host") cfg.host = value;
            else if (flag == "--port") cfg.port = parse_int(value);
            else if (flag == "--ops") cfg.ops = parse_int(value);
            else if (flag == "--threads") cfg.threads = parse_int(value);
            else if (flag == "--keys") cfg.keys = parse_int(value);
            else if (flag == "--key-size") cfg.key_size = parse_int(value);
            else if (flag == "--val-size") cfg.val_size = parse_int(value);
            else if (flag == "--warmup-ops") cfg.warmup_ops = parse_int(value);
            else if (flag == "--workload") cfg.workload = value;
            else if (flag == "--pipeline") {
                if (parse_int(value) != 1)
                    throw std::invalid_argument("only --pipeline 1 is supported: server reply ordering is not guaranteed");
            } else if (flag == "--duration-seconds") {
                size_t end = 0;
                cfg.duration_seconds = std::stod(value, &end);
                if (end != value.size() || !std::isfinite(cfg.duration_seconds) || cfg.duration_seconds <= 0)
                    throw std::invalid_argument("duration must be positive and finite");
            } else throw std::invalid_argument("unknown option: " + flag);
        }
        if (cfg.workload == "readonly") cfg.workload = "get";
        if (cfg.workload != "set" && cfg.workload != "get" && cfg.workload != "mixed")
            throw std::invalid_argument("workload must be set, get, or mixed");
        if (cfg.port < 1 || cfg.port > 65535 || cfg.threads < 1 || cfg.ops < 1 ||
            cfg.keys < 1 || cfg.val_size < 1 || cfg.val_size > 16 * 1024 * 1024 ||
            cfg.key_size < 4 + static_cast<int>(std::to_string(cfg.keys - 1).size()) ||
            cfg.key_size > 1024 || cfg.warmup_ops < 0)
            throw std::invalid_argument("invalid size/count; keys must fit without truncation");
        in_addr address{};
        if (::inet_pton(AF_INET, cfg.host.c_str(), &address) != 1)
            throw std::invalid_argument("host must be an IPv4 address");
    } catch (const std::exception& error) {
        std::cerr << "benchmark: " << error.what() << '\n';
        return 2;
    }
    const BenchResult result = run_benchmark(cfg);
    if (cfg.json) print_json(cfg, result);
    else {
        std::cout << std::fixed << std::setprecision(2)
            << "Workload: " << cfg.workload << ", clients: " << cfg.threads << ", keys: " << cfg.keys << '\n'
            << "Successful: " << result.successful << " / attempted: " << result.attempted
            << ", setup errors: " << result.setup_errors << '\n'
            << "Errors (server/protocol/transport): " << result.server_errors << '/'
            << result.protocol_errors << '/' << result.transport_errors << '\n'
            << "Successful ops/s: " << result.success_ops_per_sec << '\n'
            << "Successful-request latency (us): p50=" << result.p50_us << " p95=" << result.p95_us
            << " p99=" << result.p99_us << " p99.9=" << result.p999_us << '\n';
    }
    return result.valid ? 0 : 1;
}
