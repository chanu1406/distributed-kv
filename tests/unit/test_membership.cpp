#include "cluster/membership.h"
#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>

using namespace dkv;

TEST(MembershipTest, InitialStateIsUp) {
    Membership m;
    m.add_peer(1, "127.0.0.1:7001");
    EXPECT_EQ(m.get_state(1), NodeState::UP);
    EXPECT_TRUE(m.is_available(1));
}

TEST(MembershipTest, SuccessKeepsNodeUp) {
    Membership m;
    m.add_peer(1, "127.0.0.1:7001");
    m.record_success(1);
    m.record_success(1);
    EXPECT_EQ(m.get_state(1), NodeState::UP);
}

TEST(MembershipTest, ThreeMissesTransitionsToSuspected) {
    // suspect_threshold=3, down_threshold=60000ms (large so we don't go DOWN)
    Membership m(3, 60000);
    m.add_peer(1, "127.0.0.1:7001");

    m.record_failure(1);
    EXPECT_EQ(m.get_state(1), NodeState::UP);   // 1 miss — still UP

    m.record_failure(1);
    EXPECT_EQ(m.get_state(1), NodeState::UP);   // 2 misses — still UP

    m.record_failure(1);
    EXPECT_EQ(m.get_state(1), NodeState::SUSPECTED);  // 3rd miss
}

TEST(MembershipTest, SuspectedTransitionsToDownAfterTimeout) {
    // suspect_threshold=1, down_threshold=10ms (very short)
    Membership m(1, 10);
    m.add_peer(2, "127.0.0.1:7002");

    m.record_failure(2);  // goes SUSPECTED immediately

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    m.record_failure(2);  // now past down_threshold -> DOWN
    EXPECT_EQ(m.get_state(2), NodeState::DOWN);
    EXPECT_FALSE(m.is_available(2));
}

TEST(MembershipTest, SuccessAfterDownTransitionsBackToUp) {
    Membership m(1, 10);
    m.add_peer(3, "127.0.0.1:7003");

    m.record_failure(3);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    m.record_failure(3);
    ASSERT_EQ(m.get_state(3), NodeState::DOWN);

    m.record_success(3);
    EXPECT_EQ(m.get_state(3), NodeState::UP);
    EXPECT_TRUE(m.is_available(3));
}

TEST(MembershipTest, DownCallbackFired) {
    std::atomic<int> fired{0};
    Membership m(1, 10);
    m.set_down_callback([&](uint32_t, const std::string&) { ++fired; });

    m.add_peer(4, "127.0.0.1:7004");
    m.record_failure(4);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    m.record_failure(4);

    EXPECT_EQ(fired.load(), 1);
    // Second failure while DOWN should NOT fire again
    m.record_failure(4);
    EXPECT_EQ(fired.load(), 1);
}

TEST(MembershipTest, RejoinCallbackFired) {
    std::atomic<int> rejoin_fired{0};
    Membership m(1, 10);
    m.set_rejoin_callback([&](uint32_t, const std::string&) { ++rejoin_fired; });

    m.add_peer(5, "127.0.0.1:7005");
    m.record_failure(5);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    m.record_failure(5);
    ASSERT_EQ(m.get_state(5), NodeState::DOWN);

    m.record_success(5);
    EXPECT_EQ(rejoin_fired.load(), 1);
    EXPECT_EQ(m.get_state(5), NodeState::UP);
}

TEST(MembershipTest, SuccessResetsMissCount) {
    Membership m(3, 60000);
    m.add_peer(6, "127.0.0.1:7006");

    m.record_failure(6);
    m.record_failure(6);  // 2 misses
    EXPECT_EQ(m.get_state(6), NodeState::UP);

    m.record_success(6);  // reset

    // 3 more misses should bring to SUSPECTED
    m.record_failure(6);
    m.record_failure(6);
    m.record_failure(6);
    EXPECT_EQ(m.get_state(6), NodeState::SUSPECTED);
}

TEST(MembershipTest, DownNodesReturned) {
    Membership m(1, 10);
    m.add_peer(10, "127.0.0.1:7010");
    m.add_peer(11, "127.0.0.1:7011");

    m.record_failure(10);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    m.record_failure(10);

    auto down = m.down_nodes();
    ASSERT_EQ(down.size(), 1u);
    EXPECT_EQ(down[0], 10u);
}

TEST(MembershipTest, UnknownNodeAssumedUp) {
    Membership m;
    EXPECT_EQ(m.get_state(999), NodeState::UP);
    EXPECT_TRUE(m.is_available(999));
}
