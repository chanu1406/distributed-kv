#include <gtest/gtest.h>

#include "cluster/hash_ring.h"

#include <set>
#include <string>
#include <unordered_map>

TEST(HashRing, DeterministicLookup) {
    dkv::HashRing ring;
    ring.add_node(1, "127.0.0.1:7001");
    ring.add_node(2, "127.0.0.1:7002");
    ring.add_node(3, "127.0.0.1:7003");

    // Same key should always map to the same node
    auto n1 = ring.get_node("test_key");
    auto n2 = ring.get_node("test_key");

    ASSERT_TRUE(n1.has_value());
    ASSERT_TRUE(n2.has_value());
    EXPECT_EQ(n1->node_id, n2->node_id);
    EXPECT_EQ(n1->address, n2->address);
}

TEST(HashRing, AddRemoveNode) {
    dkv::HashRing ring;
    ring.add_node(1, "127.0.0.1:7001", 64);
    EXPECT_EQ(ring.size(), 64u);
    EXPECT_EQ(ring.node_count(), 1u);

    ring.add_node(2, "127.0.0.1:7002", 64);
    EXPECT_EQ(ring.size(), 128u);
    EXPECT_EQ(ring.node_count(), 2u);

    ring.remove_node(1);
    EXPECT_EQ(ring.size(), 64u);
    EXPECT_EQ(ring.node_count(), 1u);

    // All lookups should now go to node 2
    auto node = ring.get_node("any_key");
    ASSERT_TRUE(node.has_value());
    EXPECT_EQ(node->node_id, 2u);
}

TEST(HashRing, WrapAround) {
    dkv::HashRing ring;
    ring.add_node(1, "127.0.0.1:7001");

    // Every key should resolve, meaning wrap-around works.
    // Test a range of keys to exercise different hash positions.
    for (int i = 0; i < 100; i++) {
        auto node = ring.get_node("key_" + std::to_string(i));
        ASSERT_TRUE(node.has_value());
        EXPECT_EQ(node->node_id, 1u);
    }
}

TEST(HashRing, DistributionUniformity) {
    dkv::HashRing ring;
    ring.add_node(1, "127.0.0.1:7001");
    ring.add_node(2, "127.0.0.1:7002");
    ring.add_node(3, "127.0.0.1:7003");

    // Hash 10,000 keys and count how many go to each node
    std::unordered_map<uint32_t, int> counts;
    const int total = 10000;
    for (int i = 0; i < total; i++) {
        auto node = ring.get_node("key_" + std::to_string(i));
        ASSERT_TRUE(node.has_value());
        counts[node->node_id]++;
    }

    EXPECT_EQ(counts.size(), 3u);

    // Each node should get between 20% and 47% of keys (loose bounds to
    // avoid flakiness while still catching extreme skew)
    for (auto& [id, count] : counts) {
        double pct = static_cast<double>(count) / total;
        EXPECT_GT(pct, 0.20) << "Node " << id << " got too few keys: " << count;
        EXPECT_LT(pct, 0.47) << "Node " << id << " got too many keys: " << count;
    }
}

TEST(HashRing, GetReplicaNodes) {
    dkv::HashRing ring;
    ring.add_node(1, "127.0.0.1:7001");
    ring.add_node(2, "127.0.0.1:7002");
    ring.add_node(3, "127.0.0.1:7003");

    auto replicas = ring.get_replica_nodes("some_key", 3);
    EXPECT_EQ(replicas.size(), 3u);

    // All three should be distinct physical nodes
    std::set<uint32_t> ids;
    for (const auto& n : replicas) {
        ids.insert(n.node_id);
    }
    EXPECT_EQ(ids.size(), 3u);
}

TEST(HashRing, GetReplicaNodesInsufficientNodes) {
    dkv::HashRing ring;
    ring.add_node(1, "127.0.0.1:7001");
    ring.add_node(2, "127.0.0.1:7002");

    // Ask for 5 replicas but only 2 physical nodes exist
    auto replicas = ring.get_replica_nodes("key", 5);
    EXPECT_EQ(replicas.size(), 2u);
}

TEST(HashRing, EmptyRing) {
    dkv::HashRing ring;

    auto node = ring.get_node("key");
    EXPECT_FALSE(node.has_value());

    auto replicas = ring.get_replica_nodes("key", 3);
    EXPECT_TRUE(replicas.empty());
}
