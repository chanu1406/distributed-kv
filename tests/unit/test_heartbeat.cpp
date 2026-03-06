#include "cluster/heartbeat.h"
#include "cluster/membership.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>

#include <gtest/gtest.h>

using namespace dkv;

// ── Minimal PONG server helpers ───────────────────────────────────────────────

// Bind a listen socket and return it; set out_port to the assigned port.
static int start_pong_server(uint16_t& out_port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = 0;  // OS assigns port
    ::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    ::listen(fd, 8);

    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &len);
    out_port = ntohs(addr.sin_port);
    return fd;
}

// Accept connections and respond "+PONG\n" to any data received.
// Runs until `running` is false.
static void run_pong_server(int listen_fd, std::atomic<bool>& running) {
    ::fcntl(listen_fd, F_SETFL, O_NONBLOCK);
    while (running.load()) {
        int client = ::accept(listen_fd, nullptr, nullptr);
        if (client < 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }
        char buf[32];
        ::recv(client, buf, sizeof(buf), 0);
        ::send(client, "+PONG\n", 6, 0);
        ::close(client);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// start() / stop() can be called multiple times without crashing or deadlocking.
TEST(HeartbeatTest, StartStopIdempotent) {
    Membership m;
    m.add_peer(1, "127.0.0.1:9991");

    Heartbeat hb(m, /*node_id=*/99, /*interval_ms=*/50, /*timeout_ms=*/50);
    hb.start();
    hb.start();  // second start is a no-op
    hb.stop();
    hb.stop();   // second stop is a no-op
}

// stop() before start() must not block or crash.
TEST(HeartbeatTest, StopWithoutStartIsSafe) {
    Membership m;
    Heartbeat hb(m, 1, 50, 50);
    hb.stop();
}

// Destructor must join the background thread cleanly.
TEST(HeartbeatTest, DestructorStopsThread) {
    Membership m;
    m.add_peer(2, "127.0.0.1:9992");
    {
        Heartbeat hb(m, /*node_id=*/99, /*interval_ms=*/50, /*timeout_ms=*/50);
        hb.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        // ~Heartbeat() called here — must join, not block forever
    }
    SUCCEED();
}

// Pinging an unreachable address drives the membership state away from UP.
TEST(HeartbeatTest, DeadPeerRecordsFailures) {
    // suspect_threshold=3, very long down_threshold so node stays SUSPECTED
    Membership m(3, 60000);
    // Port 1 is always refused (privileged, nothing listening)
    m.add_peer(1, "127.0.0.1:1");

    Heartbeat hb(m, /*node_id=*/99, /*interval_ms=*/50, /*timeout_ms=*/100);
    hb.start();
    // 4 intervals → 4 consecutive failures → should reach SUSPECTED
    std::this_thread::sleep_for(std::chrono::milliseconds(350));
    hb.stop();

    EXPECT_NE(m.get_state(1), NodeState::UP);
}

// Self node_id must be skipped — no pings sent, state stays UP.
TEST(HeartbeatTest, SkipsSelf) {
    // Use a high down_threshold and suspect_threshold so even failures keep us UP for a while
    Membership m(100, 60000);
    m.add_peer(1, "127.0.0.1:1");  // would fail if pinged

    // node_id=1 matches peer id=1 → heartbeat skips this peer
    Heartbeat hb(m, /*node_id=*/1, /*interval_ms=*/50, /*timeout_ms=*/50);
    hb.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    hb.stop();

    // No failures were recorded because self was skipped
    EXPECT_EQ(m.get_state(1), NodeState::UP);
}

// stop() must return quickly even when interval_ms is very large.
TEST(HeartbeatTest, ResponsiveStop) {
    Membership m;
    m.add_peer(1, "127.0.0.1:1");

    // 5-second interval — stop() must still return within ~150ms
    Heartbeat hb(m, /*node_id=*/99, /*interval_ms=*/5000, /*timeout_ms=*/50);
    hb.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    auto t0 = std::chrono::steady_clock::now();
    hb.stop();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();

    // Sleep increments are 50ms, so worst case is one more increment after stop()
    EXPECT_LT(elapsed_ms, 200);
}

// Pinging a live server keeps the node UP.
TEST(HeartbeatTest, LivePeerRecordsSuccesses) {
    uint16_t port = 0;
    int listen_fd = start_pong_server(port);

    std::atomic<bool> srv_running{true};
    std::thread srv([&] { run_pong_server(listen_fd, srv_running); });

    Membership m;
    m.add_peer(1, "127.0.0.1:" + std::to_string(port));

    Heartbeat hb(m, /*node_id=*/99, /*interval_ms=*/50, /*timeout_ms=*/200);
    hb.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    hb.stop();

    EXPECT_EQ(m.get_state(1), NodeState::UP);

    srv_running.store(false);
    srv.join();
    ::close(listen_fd);
}

// A DOWN node that starts responding must trigger the rejoin callback
// and return to UP state.
TEST(HeartbeatTest, LivePeerTriggersRejoinCallback) {
    uint16_t port = 0;
    int listen_fd = start_pong_server(port);

    std::atomic<bool> srv_running{true};
    std::thread srv([&] { run_pong_server(listen_fd, srv_running); });

    // suspect_threshold=1, down_threshold=10ms so we can quickly drive DOWN
    Membership m(1, 10);
    std::atomic<int> rejoin_count{0};
    m.set_rejoin_callback([&](uint32_t, const std::string&) { ++rejoin_count; });

    m.add_peer(1, "127.0.0.1:" + std::to_string(port));

    // Manually drive node to DOWN
    m.record_failure(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    m.record_failure(1);
    ASSERT_EQ(m.get_state(1), NodeState::DOWN);

    // Heartbeat pings succeed → membership transitions DOWN → UP
    Heartbeat hb(m, /*node_id=*/99, /*interval_ms=*/50, /*timeout_ms=*/200);
    hb.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    hb.stop();

    EXPECT_EQ(m.get_state(1), NodeState::UP);
    EXPECT_GE(rejoin_count.load(), 1);

    srv_running.store(false);
    srv.join();
    ::close(listen_fd);
}
