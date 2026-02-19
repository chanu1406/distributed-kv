#include <gtest/gtest.h>

#include "cluster/connection_pool.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <string>
#include <thread>

namespace {

// ── Test helper: a tiny local TCP listener ───────────────────────────────────

class TestListener {
public:
    bool start(uint16_t port) {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) return false;

        int opt = 1;
        setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(port);

        if (::bind(fd_, reinterpret_cast<struct sockaddr*>(&addr),
                   sizeof(addr)) < 0) return false;
        if (::listen(fd_, 16) < 0) return false;

        return true;
    }

    /// Accept one connection and return the client fd.
    int accept_one() {
        return ::accept(fd_, nullptr, nullptr);
    }

    void stop() {
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    }

    ~TestListener() { stop(); }

private:
    int fd_ = -1;
};

// ── Test fixture ─────────────────────────────────────────────────────────────

class ConnectionPoolTest : public ::testing::Test {
protected:
    static constexpr uint16_t TEST_PORT = 19877;
    TestListener listener_;
    std::string  address_ = "127.0.0.1:" + std::to_string(TEST_PORT);

    void SetUp() override {
        ASSERT_TRUE(listener_.start(TEST_PORT));
    }

    void TearDown() override {
        listener_.stop();
    }
};

}  // namespace

// ── Test cases ───────────────────────────────────────────────────────────────

TEST_F(ConnectionPoolTest, AcquireCreatesConnection) {
    dkv::ConnectionPool pool(4, 500);

    auto conn = pool.acquire(address_);
    ASSERT_TRUE(conn.has_value());
    EXPECT_GE(conn->fd, 0);
    EXPECT_EQ(conn->address, address_);

    // Accept on server side to prevent RST
    int server_fd = listener_.accept_one();
    EXPECT_GE(server_fd, 0);
    if (server_fd >= 0) ::close(server_fd);

    ::close(conn->fd);
}

TEST_F(ConnectionPoolTest, ReleaseAndReuse) {
    dkv::ConnectionPool pool(4, 500);

    // Acquire a connection
    auto conn1 = pool.acquire(address_);
    ASSERT_TRUE(conn1.has_value());
    int first_fd = conn1->fd;

    // Accept on server side
    int server_fd = listener_.accept_one();
    EXPECT_GE(server_fd, 0);

    // Release it back to the pool
    pool.release(std::move(*conn1));

    // Acquire again — should reuse the same fd
    auto conn2 = pool.acquire(address_);
    ASSERT_TRUE(conn2.has_value());
    EXPECT_EQ(conn2->fd, first_fd);

    ::close(conn2->fd);
    if (server_fd >= 0) ::close(server_fd);
}

TEST_F(ConnectionPoolTest, CloseAll) {
    dkv::ConnectionPool pool(4, 500);

    // Acquire and release a couple of connections
    auto c1 = pool.acquire(address_);
    ASSERT_TRUE(c1.has_value());
    int s1 = listener_.accept_one();

    auto c2 = pool.acquire(address_);
    ASSERT_TRUE(c2.has_value());
    int s2 = listener_.accept_one();

    pool.release(std::move(*c1));
    pool.release(std::move(*c2));

    // Close all — subsequent acquire should create new connections
    pool.close_all();

    auto c3 = pool.acquire(address_);
    ASSERT_TRUE(c3.has_value());
    int s3 = listener_.accept_one();

    // The fd should not be the same as either closed fd (they were closed)
    // (technically possible but extremely unlikely on any OS)
    ::close(c3->fd);
    if (s1 >= 0) ::close(s1);
    if (s2 >= 0) ::close(s2);
    if (s3 >= 0) ::close(s3);
}

TEST_F(ConnectionPoolTest, MaxPoolSize) {
    // Pool max = 2 per peer
    dkv::ConnectionPool pool(2, 500);

    // Acquire 3 connections
    auto c1 = pool.acquire(address_);
    int s1 = listener_.accept_one();
    auto c2 = pool.acquire(address_);
    int s2 = listener_.accept_one();
    auto c3 = pool.acquire(address_);
    int s3 = listener_.accept_one();

    ASSERT_TRUE(c1.has_value());
    ASSERT_TRUE(c2.has_value());
    ASSERT_TRUE(c3.has_value());

    // Release all 3 — only 2 should be kept (max_per_peer = 2)
    pool.release(std::move(*c1));
    pool.release(std::move(*c2));
    pool.release(std::move(*c3));  // This one should be closed immediately

    // Acquire 2 — should reuse from pool
    auto r1 = pool.acquire(address_);
    auto r2 = pool.acquire(address_);
    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());

    // Third acquire should create a new connection (pool was drained)
    auto r3 = pool.acquire(address_);
    ASSERT_TRUE(r3.has_value());
    int s4 = listener_.accept_one();

    ::close(r1->fd);
    ::close(r2->fd);
    ::close(r3->fd);
    if (s1 >= 0) ::close(s1);
    if (s2 >= 0) ::close(s2);
    if (s3 >= 0) ::close(s3);
    if (s4 >= 0) ::close(s4);
}

TEST_F(ConnectionPoolTest, ConnectionToDeadPeer) {
    dkv::ConnectionPool pool(4, 500);

    // Try to connect to a port that has no listener
    auto conn = pool.acquire("127.0.0.1:19999");
    EXPECT_FALSE(conn.has_value());
}
