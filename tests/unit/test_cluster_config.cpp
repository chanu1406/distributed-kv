#include <gtest/gtest.h>

#include "cluster/cluster_config.h"

#include <cstdio>
#include <fstream>
#include <string>

namespace {

/// Helper: write content to a temp file and return the path.
std::string write_temp_file(const std::string& content) {
    static int counter = 0;
    std::string path = "/tmp/test_cluster_config_" + std::to_string(counter++) + ".conf";
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

}  // namespace

TEST(ClusterConfig, ParseValidFile) {
    std::string content =
        "node1 127.0.0.1:7001\n"
        "node2 127.0.0.1:7002\n"
        "node3 127.0.0.1:7003\n";

    auto path = write_temp_file(content);
    auto entries = dkv::parse_cluster_config(path);
    std::remove(path.c_str());

    ASSERT_EQ(entries.size(), 3u);

    EXPECT_EQ(entries[0].name, "node1");
    EXPECT_EQ(entries[0].host, "127.0.0.1");
    EXPECT_EQ(entries[0].port, 7001);

    EXPECT_EQ(entries[1].name, "node2");
    EXPECT_EQ(entries[1].host, "127.0.0.1");
    EXPECT_EQ(entries[1].port, 7002);

    EXPECT_EQ(entries[2].name, "node3");
    EXPECT_EQ(entries[2].host, "127.0.0.1");
    EXPECT_EQ(entries[2].port, 7003);
}

TEST(ClusterConfig, SkipsComments) {
    std::string content =
        "# This is a comment\n"
        "node1 127.0.0.1:7001\n"
        "# Another comment\n"
        "node2 127.0.0.1:7002\n";

    auto path = write_temp_file(content);
    auto entries = dkv::parse_cluster_config(path);
    std::remove(path.c_str());

    ASSERT_EQ(entries.size(), 2u);
    EXPECT_EQ(entries[0].name, "node1");
    EXPECT_EQ(entries[1].name, "node2");
}

TEST(ClusterConfig, SkipsBlankLines) {
    std::string content =
        "\n"
        "node1 127.0.0.1:7001\n"
        "\n"
        "   \n"
        "node2 127.0.0.1:7002\n"
        "\n";

    auto path = write_temp_file(content);
    auto entries = dkv::parse_cluster_config(path);
    std::remove(path.c_str());

    ASSERT_EQ(entries.size(), 2u);
    EXPECT_EQ(entries[0].name, "node1");
    EXPECT_EQ(entries[1].name, "node2");
}

TEST(ClusterConfig, SkipsMalformedLines) {
    std::string content =
        "node1 127.0.0.1:7001\n"
        "this_is_bad_no_address\n"
        "node2 missing_port\n"
        "node3 127.0.0.1:7003\n";

    auto path = write_temp_file(content);
    auto entries = dkv::parse_cluster_config(path);
    std::remove(path.c_str());

    // Should skip the two malformed lines and keep node1 + node3
    ASSERT_EQ(entries.size(), 2u);
    EXPECT_EQ(entries[0].name, "node1");
    EXPECT_EQ(entries[0].port, 7001);
    EXPECT_EQ(entries[1].name, "node3");
    EXPECT_EQ(entries[1].port, 7003);
}

TEST(ClusterConfig, MissingFile) {
    auto entries = dkv::parse_cluster_config("/tmp/nonexistent_cluster_file_999.conf");
    EXPECT_TRUE(entries.empty());
}
