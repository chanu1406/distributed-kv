#include <gtest/gtest.h>

#include "config/config.h"

TEST(Config, Defaults) {
    char prog[] = "dkv_node";
    char* argv[] = {prog};
    auto cfg = dkv::parse_args(1, argv);

    EXPECT_EQ(cfg.node_id, 1u);
    EXPECT_EQ(cfg.port, 7001);
    EXPECT_EQ(cfg.replication_factor, 3u);
    EXPECT_EQ(cfg.write_quorum, 2u);
    EXPECT_EQ(cfg.read_quorum, 2u);
    EXPECT_EQ(cfg.vnodes, 128u);
    EXPECT_EQ(cfg.worker_threads, 4u);
}

TEST(Config, ParsePort) {
    char prog[] = "dkv_node";
    char flag[] = "--port";
    char val[]  = "9000";
    char* argv[] = {prog, flag, val};
    auto cfg = dkv::parse_args(3, argv);

    EXPECT_EQ(cfg.port, 9000);
}

TEST(Config, ParseNodeId) {
    char prog[] = "dkv_node";
    char flag[] = "--node-id";
    char val[]  = "5";
    char* argv[] = {prog, flag, val};
    auto cfg = dkv::parse_args(3, argv);

    EXPECT_EQ(cfg.node_id, 5u);
}

TEST(Config, ParseMultipleFlags) {
    char prog[] = "dkv_node";
    char f1[]   = "--port";
    char v1[]   = "8080";
    char f2[]   = "--vnodes";
    char v2[]   = "256";
    char f3[]   = "--write-quorum";
    char v3[]   = "1";
    char* argv[] = {prog, f1, v1, f2, v2, f3, v3};
    auto cfg = dkv::parse_args(7, argv);

    EXPECT_EQ(cfg.port, 8080);
    EXPECT_EQ(cfg.vnodes, 256u);
    EXPECT_EQ(cfg.write_quorum, 1u);
}

TEST(Config, QuorumInvariant) {
    // W + R > N should hold with defaults (2 + 2 > 3)
    char prog[] = "dkv_node";
    char* argv[] = {prog};
    auto cfg = dkv::parse_args(1, argv);

    EXPECT_GT(cfg.write_quorum + cfg.read_quorum, cfg.replication_factor);
}
