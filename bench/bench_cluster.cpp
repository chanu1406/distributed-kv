// bench_cluster.cpp — End-to-end cluster benchmark
// Measures TCP throughput and latency across a live running DKV cluster.
//
// Usage: ./bin/bench_cluster [--host H] [--port P] [--ops N] [--threads N]
//                            [--pipeline N] [--key-size N] [--val-size N]
//                            [--workload set|get|mixed|readonly] [--warmup-ops N]

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// POSIX socket headers (Linux / macOS only)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

using namespace std::chrono;

// ── Key / Value helpers ────────────────────────────────────────────────────────

static std::string make_key(int i, int key_size) {
    std::string k = "key:" + std::to_string(i);
    while (static_cast<int>(k.size()) < key_size) k += 'x';
    return k.substr(0, static_cast<size_t>(key_size));
}

static std::string make_val(int val_size) {
    return std::string(static_cast<size_t>(val_size), 'v');
}

// ── Latency stats (identical pattern to bench_storage.cpp) ────────────────────

struct Stats {
    std::vector<int64_t> latencies_ns;

    void reserve(size_t n) { latencies_ns.reserve(n); }

    void record(nanoseconds ns) {
        latencies_ns.push_back(ns.count());
    }

    void merge(const Stats& other) {
        latencies_ns.insert(latencies_ns.end(),
                            other.latencies_ns.begin(),
                            other.latencies_ns.end());
    }

    // Returns percentile (0-100) in microseconds.
    double percentile(int p) {
        if (latencies_ns.empty()) return 0;
        std::sort(latencies_ns.begin(), latencies_ns.end());
        size_t idx = static_cast<size_t>(p / 100.0 * latencies_ns.size());
        if (idx >= latencies_ns.size()) idx = latencies_ns.size() - 1;
        return latencies_ns[idx] / 1000.0;
    }

    double mean_us() {
        if (latencies_ns.empty()) return 0;
        double sum = 0;
        for (auto v : latencies_ns) sum += static_cast<double>(v);
        return (sum / static_cast<double>(latencies_ns.size())) / 1000.0;
    }
};

// ── Config / Result ───────────────────────────────────────────────────────────

struct BenchConfig {
    std::string host       = "127.0.0.1";
    int         port       = 7001;
    int         ops        = 100000;
    int         threads    = 1;
    int         pipeline   = 1;
    int         key_size   = 16;
    int         val_size   = 64;
    std::string workload   = "set";   // set | get | mixed | readonly
    int         warmup_ops = 1000;
};

struct BenchResult {
    std::string name;
    double      ops_per_sec = 0;
    double      mean_us     = 0;
    double      p50_us      = 0;
    double      p99_us      = 0;
    double      p999_us     = 0;
    int         errors      = 0;
    int         total_ops   = 0;
};

// ── TCP helpers ───────────────────────────────────────────────────────────────

static int open_connection(const std::string& host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    // Disable Nagle's algorithm for lower latency on small frames.
    int one = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                     &one, static_cast<socklen_t>(sizeof(one)));

    // 2-second send / receive timeout.
    struct timeval tv{};
    tv.tv_sec  = 2;
    tv.tv_usec = 0;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,
                     &tv, static_cast<socklen_t>(sizeof(tv)));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO,
                     &tv, static_cast<socklen_t>(sizeof(tv)));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }
    if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr),
                static_cast<socklen_t>(sizeof(addr))) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// Send exactly `len` bytes; returns false on failure.
static bool send_all(int fd, const char* buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// Read until '\n'; returns the full line including '\n', or "" on error.
static std::string recv_line(int fd) {
    std::string line;
    line.reserve(128);
    char c = '\0';
    while (true) {
        ssize_t n = recv(fd, &c, 1, 0);
        if (n <= 0) return "";
        line += c;
        if (c == '\n') return line;
    }
}

// ── Protocol formatters (client-side; no dkv_core dependency) ─────────────────

static std::string fmt_set(const std::string& key, const std::string& val) {
    return "SET " + std::to_string(key.size()) + " " + key + " "
         + std::to_string(val.size()) + " " + val + "\n";
}

static std::string fmt_get(const std::string& key) {
    return "GET " + std::to_string(key.size()) + " " + key + "\n";
}

static bool is_error_response(const std::string& resp) {
    return !resp.empty() && resp[0] == '-';
}

// ── Per-thread accumulated state ──────────────────────────────────────────────

struct ThreadState {
    Stats stats;
    int   errors    = 0;
    int   total_ops = 0;
};

// Run `count` operations on `fd` starting at key index `key_base`.
// When `record` is false (warmup), latencies and error counts are discarded.
// Returns false if the TCP connection is lost partway through.
static bool run_ops(int fd, const BenchConfig& cfg,
                    int key_base, int count, bool record,
                    ThreadState& state) {
    const std::string val = make_val(cfg.val_size);

    int i = 0;
    while (i < count) {
        int batch = std::min(cfg.pipeline, count - i);

        auto t0 = high_resolution_clock::now();

        // ── Send batch ────────────────────────────────────────────────────────
        int sent = 0;
        for (int b = 0; b < batch; ++b) {
            int abs_idx = key_base + i + b;
            std::string req;

            if (cfg.workload == "set") {
                req = fmt_set(make_key(abs_idx, cfg.key_size), val);
            } else if (cfg.workload == "get" || cfg.workload == "readonly") {
                req = fmt_get(make_key(abs_idx, cfg.key_size));
            } else {
                // mixed: even ops → SET key i, odd ops → GET key i-1
                if ((i + b) % 2 == 0) {
                    req = fmt_set(make_key(abs_idx, cfg.key_size), val);
                } else {
                    int get_idx = abs_idx > 0 ? abs_idx - 1 : abs_idx;
                    req = fmt_get(make_key(get_idx, cfg.key_size));
                }
            }

            if (!send_all(fd, req.c_str(), req.size())) break;
            ++sent;
        }

        if (sent == 0) return false;  // connection lost before anything was sent

        // ── Receive responses for every successfully-sent request ─────────────
        for (int b = 0; b < sent; ++b) {
            std::string resp = recv_line(fd);
            if (record) {
                ++state.total_ops;
                if (resp.empty() || is_error_response(resp)) ++state.errors;
            }
        }

        auto t1 = high_resolution_clock::now();
        if (record) {
            // Batch-level latency granularity (per spec for pipeline > 1).
            state.stats.record(duration_cast<nanoseconds>(t1 - t0));
        }

        if (sent < batch) return false;  // partial send → connection dead

        i += batch;
    }
    return true;
}

// ── Benchmark orchestration ───────────────────────────────────────────────────

static BenchResult run_benchmark(const BenchConfig& cfg) {
    // Build the result name shown in the output table.
    std::string name;
    if (cfg.workload == "mixed") {
        name = "MIXED 50/50 (" + std::to_string(cfg.threads) + " thread"
             + (cfg.threads > 1 ? "s" : "") + ")";
    } else {
        std::string label = (cfg.workload == "set") ? "SET" : "GET";
        name = label + " (" + std::to_string(cfg.threads) + " thread"
             + (cfg.threads > 1 ? "s" : "") + ", pipeline="
             + std::to_string(cfg.pipeline) + ")";
    }

    int ops_per_thread = cfg.ops / cfg.threads;

    std::vector<ThreadState>  thread_states(static_cast<size_t>(cfg.threads));
    std::atomic<int>          ready_count{0};
    std::atomic<bool>         go{false};

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(cfg.threads));

    for (int t = 0; t < cfg.threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& state  = thread_states[static_cast<size_t>(t)];
            int key_base = t * ops_per_thread;

            int fd = open_connection(cfg.host, cfg.port);
            if (fd < 0) {
                state.errors    = ops_per_thread;
                state.total_ops = ops_per_thread;
                ready_count.fetch_add(1, std::memory_order_release);
                return;
            }

            // Pre-populate keys for get / readonly (untimed, pipeline=1 for safety).
            if (cfg.workload == "get" || cfg.workload == "readonly") {
                BenchConfig pop = cfg;
                pop.workload    = "set";
                pop.pipeline    = 1;
                ThreadState dummy;
                run_ops(fd, pop, key_base, ops_per_thread, false, dummy);
            }

            // Warmup phase — same workload, not recorded.
            if (cfg.warmup_ops > 0) {
                ThreadState dummy;
                run_ops(fd, cfg, key_base,
                        std::min(cfg.warmup_ops, ops_per_thread),
                        false, dummy);
            }

            // Signal ready and spin-wait for the go flag.
            ready_count.fetch_add(1, std::memory_order_release);
            while (!go.load(std::memory_order_acquire))
                std::this_thread::yield();

            // Timed benchmark phase.
            size_t sample_count = static_cast<size_t>(
                (ops_per_thread + cfg.pipeline - 1) / cfg.pipeline);
            state.stats.reserve(sample_count);
            run_ops(fd, cfg, key_base, ops_per_thread, true, state);

            close(fd);
        });
    }

    // Wait until all threads are ready, then start the clock and fire.
    while (ready_count.load(std::memory_order_acquire) < cfg.threads)
        std::this_thread::yield();

    auto wall_start = high_resolution_clock::now();
    go.store(true, std::memory_order_release);

    for (auto& th : threads) th.join();
    auto wall_end = high_resolution_clock::now();

    // Aggregate latency samples and error counts from all threads.
    Stats combined;
    combined.reserve(static_cast<size_t>(cfg.ops));
    int total_errors = 0;
    int total_ops    = 0;
    for (auto& s : thread_states) {
        combined.merge(s.stats);
        total_errors += s.errors;
        total_ops    += s.total_ops;
    }

    double elapsed_s = duration_cast<microseconds>(wall_end - wall_start).count() / 1e6;
    int    measured  = ops_per_thread * cfg.threads;

    return {
        name,
        measured / elapsed_s,
        combined.mean_us(),
        combined.percentile(50),
        combined.percentile(99),
        combined.percentile(99'9),
        total_errors,
        total_ops
    };
}

// ── Output (matches bench_storage.cpp column layout exactly) ──────────────────

static void print_header() {
    std::cout << "\n";
    std::cout << std::left
              << std::setw(34) << "Benchmark"
              << std::right
              << std::setw(14) << "Throughput"
              << std::setw(13) << "Mean"
              << std::setw(13) << "P50"
              << std::setw(13) << "P99"
              << std::setw(13) << "P99.9"
              << "\n";
    std::cout << std::string(100, '-') << "\n";
}

static void print_result(const BenchResult& r) {
    auto fmt_ops = [](double ops) -> std::string {
        std::ostringstream oss;
        if (ops >= 1e6)      oss << std::fixed << std::setprecision(2) << ops / 1e6 << " Mops/s";
        else if (ops >= 1e3) oss << std::fixed << std::setprecision(2) << ops / 1e3 << " Kops/s";
        else                 oss << std::fixed << std::setprecision(0) << ops << " ops/s";
        return oss.str();
    };
    auto fmt_us = [](double us) -> std::string {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << us << " µs";
        return oss.str();
    };

    std::cout << std::left  << std::setw(34) << r.name
              << std::right << std::setw(14) << fmt_ops(r.ops_per_sec)
              << std::setw(13) << fmt_us(r.mean_us)
              << std::setw(13) << fmt_us(r.p50_us)
              << std::setw(13) << fmt_us(r.p99_us)
              << std::setw(13) << fmt_us(r.p999_us)
              << "\n";
    if (r.errors > 0) {
        std::cout << "  Errors: " << r.errors << " / " << r.total_ops << " ops\n";
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    BenchConfig cfg;

    for (int i = 1; i < argc; ++i) {
        if      (std::strcmp(argv[i], "--host")        == 0 && i + 1 < argc)
            cfg.host       = argv[++i];
        else if (std::strcmp(argv[i], "--port")        == 0 && i + 1 < argc)
            cfg.port       = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--ops")         == 0 && i + 1 < argc)
            cfg.ops        = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--threads")     == 0 && i + 1 < argc)
            cfg.threads    = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--pipeline")    == 0 && i + 1 < argc)
            cfg.pipeline   = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--key-size")    == 0 && i + 1 < argc)
            cfg.key_size   = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--val-size")    == 0 && i + 1 < argc)
            cfg.val_size   = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--workload")    == 0 && i + 1 < argc)
            cfg.workload   = argv[++i];
        else if (std::strcmp(argv[i], "--warmup-ops")  == 0 && i + 1 < argc)
            cfg.warmup_ops = std::atoi(argv[++i]);
    }

    if (cfg.threads  < 1) cfg.threads  = 1;
    if (cfg.ops      < 1) cfg.ops      = 1;
    if (cfg.pipeline < 1) cfg.pipeline = 1;

    std::cout << "DKV Cluster Benchmark\n";
    std::cout << "  target="   << cfg.host << ":" << cfg.port
              << "  ops="      << cfg.ops
              << "  threads="  << cfg.threads
              << "  pipeline=" << cfg.pipeline
              << "  key_size=" << cfg.key_size << "B"
              << "  val_size=" << cfg.val_size << "B"
              << "  workload=" << cfg.workload << "\n";

    print_header();
    BenchResult result = run_benchmark(cfg);
    print_result(result);
    std::cout << std::string(100, '-') << "\n";

    return 0;
}
