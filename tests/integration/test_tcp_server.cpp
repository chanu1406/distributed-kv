#include <gtest/gtest.h>

#include "network/tcp_server.h"
#include "storage/storage_engine.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <string>
#include <thread>

namespace {

// ── Test helper: a tiny blocking TCP client ──────────────────────────────────

class TestClient {
public:
    bool connect_to(uint16_t port) {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) return false;

        struct sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(port);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");

        return ::connect(fd_, reinterpret_cast<struct sockaddr*>(&addr),
                         sizeof(addr)) == 0;
    }

    bool send_data(const std::string& data) {
        ssize_t sent = ::send(fd_, data.data(), data.size(), 0);
        return sent == static_cast<ssize_t>(data.size());
    }

    std::string recv_data(size_t max_len = 4096, int timeout_ms = 2000) {
        // Set receive timeout
        struct timeval tv;
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        std::string result;
        char buf[4096];
        while (result.size() < max_len) {
            ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
            if (n <= 0) break;
            result.append(buf, static_cast<size_t>(n));
        }
        return result;
    }

    /// Receive exactly `expected_count` newline-terminated responses.
    std::string recv_responses(int expected_count, int timeout_ms = 2000) {
        struct timeval tv;
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        std::string result;
        int newlines = 0;
        char buf[4096];
        while (newlines < expected_count) {
            ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
            if (n <= 0) break;
            for (ssize_t i = 0; i < n; i++) {
                result += buf[i];
                if (buf[i] == '\n') newlines++;
            }
        }
        return result;
    }

    void close_conn() {
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    }

    ~TestClient() { close_conn(); }

private:
    int fd_ = -1;
};

// ── Test fixture: spins up server on an ephemeral port ───────────────────────

class TCPIntegrationTest : public ::testing::Test {
protected:
    static constexpr uint16_t TEST_PORT = 19876;
    dkv::StorageEngine engine_;
    std::unique_ptr<dkv::TCPServer> server_;
    std::thread server_thread_;

    void SetUp() override {
        server_ = std::make_unique<dkv::TCPServer>(engine_, TEST_PORT, 2);
        server_thread_ = std::thread([this]() { server_->run(); });
        // Give the server time to bind and start listening
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown() override {
        server_->stop();
        if (server_thread_.joinable()) server_thread_.join();
    }
};

}  // namespace

// ── Test cases ───────────────────────────────────────────────────────────────

TEST_F(TCPIntegrationTest, Ping) {
    TestClient client;
    ASSERT_TRUE(client.connect_to(TEST_PORT));

    ASSERT_TRUE(client.send_data("PING\n"));
    std::string resp = client.recv_responses(1);
    EXPECT_EQ(resp, "+PONG\n");
}

TEST_F(TCPIntegrationTest, SetGetRoundTrip) {
    TestClient client;
    ASSERT_TRUE(client.connect_to(TEST_PORT));

    ASSERT_TRUE(client.send_data("SET 5 mykey 7 myvalue\n"));
    std::string resp1 = client.recv_responses(1);
    EXPECT_EQ(resp1, "+OK\n");

    ASSERT_TRUE(client.send_data("GET 5 mykey\n"));
    std::string resp2 = client.recv_responses(1);
    EXPECT_EQ(resp2, "$7 myvalue\n");
}

TEST_F(TCPIntegrationTest, GetNotFound) {
    TestClient client;
    ASSERT_TRUE(client.connect_to(TEST_PORT));

    ASSERT_TRUE(client.send_data("GET 11 nonexistent\n"));
    std::string resp = client.recv_responses(1);
    EXPECT_EQ(resp, "-NOT_FOUND\n");
}

TEST_F(TCPIntegrationTest, SetGetDel) {
    TestClient client;
    ASSERT_TRUE(client.connect_to(TEST_PORT));

    // SET
    ASSERT_TRUE(client.send_data("SET 3 foo 3 bar\n"));
    EXPECT_EQ(client.recv_responses(1), "+OK\n");

    // GET
    ASSERT_TRUE(client.send_data("GET 3 foo\n"));
    EXPECT_EQ(client.recv_responses(1), "$3 bar\n");

    // Small delay so DEL timestamp > SET timestamp under LWW
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    // DEL
    ASSERT_TRUE(client.send_data("DEL 3 foo\n"));
    EXPECT_EQ(client.recv_responses(1), "+OK\n");

    // GET after DEL
    ASSERT_TRUE(client.send_data("GET 3 foo\n"));
    EXPECT_EQ(client.recv_responses(1), "-NOT_FOUND\n");
}

TEST_F(TCPIntegrationTest, MalformedCommandReturnsError) {
    TestClient client;
    ASSERT_TRUE(client.connect_to(TEST_PORT));

    ASSERT_TRUE(client.send_data("FOOBAR\n"));
    std::string resp = client.recv_responses(1);
    EXPECT_TRUE(resp.find("-ERR") != std::string::npos);
}

TEST_F(TCPIntegrationTest, PartialFrameSend) {
    // Classic reactor pitfall: send half a command, then the rest
    TestClient client;
    ASSERT_TRUE(client.connect_to(TEST_PORT));

    ASSERT_TRUE(client.send_data("SET 3 foo"));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_TRUE(client.send_data(" 3 bar\n"));

    std::string resp = client.recv_responses(1);
    EXPECT_EQ(resp, "+OK\n");
}

TEST_F(TCPIntegrationTest, PipelinedRequests) {
    // Multiple frames in a single send()
    TestClient client;
    ASSERT_TRUE(client.connect_to(TEST_PORT));

    std::string pipeline = "PING\nPING\nPING\n";
    ASSERT_TRUE(client.send_data(pipeline));

    std::string resp = client.recv_responses(3);
    EXPECT_EQ(resp, "+PONG\n+PONG\n+PONG\n");
}

TEST_F(TCPIntegrationTest, LargePayload) {
    TestClient client;
    ASSERT_TRUE(client.connect_to(TEST_PORT));

    std::string large_val(100000, 'X');  // 100KB
    std::string cmd = "SET 7 bigdata " + std::to_string(large_val.size())
                    + " " + large_val + "\n";

    ASSERT_TRUE(client.send_data(cmd));
    EXPECT_EQ(client.recv_responses(1), "+OK\n");

    ASSERT_TRUE(client.send_data("GET 7 bigdata\n"));
    std::string resp = client.recv_responses(1, 5000);
    std::string expected = "$" + std::to_string(large_val.size())
                         + " " + large_val + "\n";
    EXPECT_EQ(resp, expected);
}

TEST_F(TCPIntegrationTest, MultipleClients) {
    TestClient c1, c2;
    ASSERT_TRUE(c1.connect_to(TEST_PORT));
    ASSERT_TRUE(c2.connect_to(TEST_PORT));

    // Client 1 sets a key
    ASSERT_TRUE(c1.send_data("SET 9 sharedkey 6 value1\n"));
    EXPECT_EQ(c1.recv_responses(1), "+OK\n");

    // Client 2 reads the key set by client 1
    // Small delay to ensure the SET has been processed
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_TRUE(c2.send_data("GET 9 sharedkey\n"));
    EXPECT_EQ(c2.recv_responses(1), "$6 value1\n");
}
