// bench_storage.cpp — Storage engine microbenchmark
// Measures raw SET/GET/DEL throughput and latency percentiles
// without any network overhead.
//
// Usage: ./bin/bench_storage [--ops N] [--threads N] [--key-size N] [--val-size N]

#include "storage/storage_engine.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono;

// ── Helpers ───────────────────────────────────────────────────────────────────

static std::string make_key(int i, int key_size) {
    std::string k = "key:" + std::to_string(i);
    while (static_cast<int>(k.size()) < key_size) k += 'x';
    return k.substr(0, static_cast<size_t>(key_size));
}

static std::string make_val(int val_size) {
    return std::string(static_cast<size_t>(val_size), 'v');
}

// ── Latency histogram ─────────────────────────────────────────────────────────

struct Stats {
    std::vector<int64_t> latencies_ns;  // per-op latency in nanoseconds

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
        for (auto v : latencies_ns) sum += v;
        return (sum / latencies_ns.size()) / 1000.0;
    }
};

// ── Benchmark runners ─────────────────────────────────────────────────────────

struct BenchConfig {
    int ops        = 100000;
    int threads    = 1;
    int key_size   = 16;
    int val_size   = 64;
};

struct BenchResult {
    std::string name;
    double      ops_per_sec;
    double      mean_us;
    double      p50_us;
    double      p99_us;
    double      p999_us;
};

// Single-threaded SET benchmark
BenchResult bench_set(dkv::StorageEngine& engine, const BenchConfig& cfg) {
    Stats stats;
    stats.reserve(static_cast<size_t>(cfg.ops));

    std::string val = make_val(cfg.val_size);
    dkv::Version ver{1000, 1};

    // Warmup
    for (int i = 0; i < std::min(cfg.ops / 10, 1000); ++i) {
        engine.set(make_key(i, cfg.key_size), val, ver);
    }

    auto t0 = high_resolution_clock::now();
    for (int i = 0; i < cfg.ops; ++i) {
        auto op_start = high_resolution_clock::now();
        engine.set(make_key(i, cfg.key_size), val, ver);
        auto op_end   = high_resolution_clock::now();
        stats.record(duration_cast<nanoseconds>(op_end - op_start));
    }
    auto t1 = high_resolution_clock::now();

    double elapsed_s = duration_cast<microseconds>(t1 - t0).count() / 1e6;
    return {
        "SET (1 thread)",
        cfg.ops / elapsed_s,
        stats.mean_us(),
        stats.percentile(50),
        stats.percentile(99),
        stats.percentile(99'9)
    };
}

// Single-threaded GET benchmark (keys inserted first)
BenchResult bench_get(dkv::StorageEngine& engine, const BenchConfig& cfg) {
    // Pre-populate
    std::string val = make_val(cfg.val_size);
    dkv::Version ver{1000, 1};
    for (int i = 0; i < cfg.ops; ++i) {
        engine.set(make_key(i, cfg.key_size), val, ver);
    }

    Stats stats;
    stats.reserve(static_cast<size_t>(cfg.ops));

    auto t0 = high_resolution_clock::now();
    for (int i = 0; i < cfg.ops; ++i) {
        auto op_start = high_resolution_clock::now();
        auto result   = engine.get(make_key(i, cfg.key_size));
        (void)result;
        auto op_end   = high_resolution_clock::now();
        stats.record(duration_cast<nanoseconds>(op_end - op_start));
    }
    auto t1 = high_resolution_clock::now();

    double elapsed_s = duration_cast<microseconds>(t1 - t0).count() / 1e6;
    return {
        "GET (1 thread)",
        cfg.ops / elapsed_s,
        stats.mean_us(),
        stats.percentile(50),
        stats.percentile(99),
        stats.percentile(99'9)
    };
}

// Multi-threaded SET benchmark
BenchResult bench_set_mt(dkv::StorageEngine& engine, const BenchConfig& cfg) {
    int ops_per_thread = cfg.ops / cfg.threads;
    std::vector<Stats> per_thread_stats(static_cast<size_t>(cfg.threads));
    for (auto& s : per_thread_stats) s.reserve(static_cast<size_t>(ops_per_thread));

    std::string val = make_val(cfg.val_size);
    dkv::Version ver{1000, 1};

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(cfg.threads));

    auto t0 = high_resolution_clock::now();
    for (int t = 0; t < cfg.threads; ++t) {
        threads.emplace_back([&, t]() {
            int base = t * ops_per_thread;
            for (int i = 0; i < ops_per_thread; ++i) {
                auto op_start = high_resolution_clock::now();
                engine.set(make_key(base + i, cfg.key_size), val, ver);
                auto op_end   = high_resolution_clock::now();
                per_thread_stats[static_cast<size_t>(t)].record(
                    duration_cast<nanoseconds>(op_end - op_start));
            }
        });
    }
    for (auto& th : threads) th.join();
    auto t1 = high_resolution_clock::now();

    Stats combined;
    combined.reserve(static_cast<size_t>(cfg.ops));
    for (auto& s : per_thread_stats) combined.merge(s);

    int total_ops    = ops_per_thread * cfg.threads;
    double elapsed_s = duration_cast<microseconds>(t1 - t0).count() / 1e6;
    return {
        "SET (" + std::to_string(cfg.threads) + " threads)",
        total_ops / elapsed_s,
        combined.mean_us(),
        combined.percentile(50),
        combined.percentile(99),
        combined.percentile(99'9)
    };
}

// Multi-threaded GET benchmark (50% read / 50% write mixed)
BenchResult bench_mixed_mt(dkv::StorageEngine& engine, const BenchConfig& cfg) {
    // Pre-populate half the keys
    std::string val = make_val(cfg.val_size);
    dkv::Version ver{1000, 1};
    for (int i = 0; i < cfg.ops / 2; ++i) {
        engine.set(make_key(i, cfg.key_size), val, ver);
    }

    int ops_per_thread = cfg.ops / cfg.threads;
    std::vector<Stats> per_thread_stats(static_cast<size_t>(cfg.threads));
    for (auto& s : per_thread_stats) s.reserve(static_cast<size_t>(ops_per_thread));

    std::atomic<int> counter{0};
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(cfg.threads));

    auto t0 = high_resolution_clock::now();
    for (int t = 0; t < cfg.threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                int key_idx   = counter.fetch_add(1, std::memory_order_relaxed);
                auto op_start = high_resolution_clock::now();
                if (i % 2 == 0) {
                    engine.get(make_key(key_idx % (cfg.ops / 2), cfg.key_size));
                } else {
                    engine.set(make_key(key_idx, cfg.key_size), val, ver);
                }
                auto op_end = high_resolution_clock::now();
                per_thread_stats[static_cast<size_t>(t)].record(
                    duration_cast<nanoseconds>(op_end - op_start));
            }
        });
    }
    for (auto& th : threads) th.join();
    auto t1 = high_resolution_clock::now();

    Stats combined;
    combined.reserve(static_cast<size_t>(cfg.ops));
    for (auto& s : per_thread_stats) combined.merge(s);

    int total_ops    = ops_per_thread * cfg.threads;
    double elapsed_s = duration_cast<microseconds>(t1 - t0).count() / 1e6;
    return {
        "MIXED 50/50 (" + std::to_string(cfg.threads) + " threads)",
        total_ops / elapsed_s,
        combined.mean_us(),
        combined.percentile(50),
        combined.percentile(99),
        combined.percentile(99'9)
    };
}

// ── Output ────────────────────────────────────────────────────────────────────

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
}

// ── Main ──────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    BenchConfig cfg;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--ops") == 0 && i + 1 < argc)
            cfg.ops = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--threads") == 0 && i + 1 < argc)
            cfg.threads = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--key-size") == 0 && i + 1 < argc)
            cfg.key_size = std::atoi(argv[++i]);
        else if (std::strcmp(argv[i], "--val-size") == 0 && i + 1 < argc)
            cfg.val_size = std::atoi(argv[++i]);
    }

    if (cfg.threads < 1) cfg.threads = 1;
    if (cfg.ops < 1)     cfg.ops     = 1;

    std::cout << "DKV Storage Engine Benchmark\n";
    std::cout << "  ops=" << cfg.ops
              << "  threads=" << cfg.threads
              << "  key_size=" << cfg.key_size << "B"
              << "  val_size=" << cfg.val_size << "B\n";

    print_header();

    {
        dkv::StorageEngine engine;
        print_result(bench_set(engine, cfg));
    }
    {
        dkv::StorageEngine engine;
        print_result(bench_get(engine, cfg));
    }
    if (cfg.threads > 1) {
        {
            dkv::StorageEngine engine;
            print_result(bench_set_mt(engine, cfg));
        }
        {
            dkv::StorageEngine engine;
            print_result(bench_mixed_mt(engine, cfg));
        }
    } else {
        // Also run 4-thread versions for comparison
        BenchConfig mt_cfg = cfg;
        mt_cfg.threads = 4;
        {
            dkv::StorageEngine engine;
            print_result(bench_set_mt(engine, mt_cfg));
        }
        {
            dkv::StorageEngine engine;
            print_result(bench_mixed_mt(engine, mt_cfg));
        }
    }

    std::cout << std::string(100, '-') << "\n";
    return 0;
}
