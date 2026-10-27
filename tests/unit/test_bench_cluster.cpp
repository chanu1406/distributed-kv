// Exercise the actual standalone benchmark helpers without a second main.
#define main benchmark_cli_main
#include "../../bench/bench_cluster.cpp"
#undef main

#include <gtest/gtest.h>

TEST(BenchmarkStats, FractionalPercentileIsNotMaximumOrP99) {
    Stats stats;
    for (int i = 10000; i >= 1; --i) stats.record(std::chrono::microseconds(i));
    double p = 99.9;
    EXPECT_DOUBLE_EQ(stats.percentile(p), 9990.0);
}

TEST(BenchmarkStats, UsesNearestRankAtExactBoundary) {
    Stats stats;
    for (int i = 1; i <= 100; ++i) stats.record(std::chrono::microseconds(i));
    EXPECT_DOUBLE_EQ(stats.percentile(50), 50.0);
    EXPECT_DOUBLE_EQ(stats.percentile(95), 95.0);
    EXPECT_DOUBLE_EQ(stats.percentile(99), 99.0);
}

TEST(BenchmarkStats, EmptyAndSingleSampleAreDefined) {
    Stats stats;
    double p = 99.9;
    EXPECT_DOUBLE_EQ(stats.percentile(p), 0.0);
    stats.record(std::chrono::microseconds(123));
    EXPECT_DOUBLE_EQ(stats.percentile(p), 123.0);
}
