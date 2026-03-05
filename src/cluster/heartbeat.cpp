#include "cluster/heartbeat.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

namespace dkv {

Heartbeat::Heartbeat(Membership& membership, uint32_t node_id,
                     int interval_ms, int timeout_ms)
    : membership_(membership),
      node_id_(node_id),
      interval_ms_(interval_ms),
      timeout_ms_(timeout_ms) {}

Heartbeat::~Heartbeat() {
    stop();
}

void Heartbeat::start() {
    if (running_.exchange(true)) return;  // already running
    thread_ = std::thread([this] { run(); });
}

void Heartbeat::stop() {
    if (!running_.exchange(false)) return;  // already stopped
    if (thread_.joinable()) thread_.join();
}

void Heartbeat::run() {
    while (running_.load()) {
        auto peers = membership_.all_peers();

        for (const auto& peer : peers) {
            if (!running_.load()) break;
            if (peer.node_id == node_id_) continue;  // skip self

            bool alive = ping_node(peer.address);
            if (alive) {
                membership_.record_success(peer.node_id);
            } else {
                membership_.record_failure(peer.node_id);
            }
        }

        // Sleep in small increments so stop() is responsive.
        int elapsed = 0;
        while (running_.load() && elapsed < interval_ms_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            elapsed += 50;
        }
    }
}

bool Heartbeat::ping_node(const std::string& address) const {
    // Parse "host:port"
    auto colon = address.rfind(':');
    if (colon == std::string::npos) return false;

    std::string host = address.substr(0, colon);
    int port = 0;
    try {
        port = std::stoi(address.substr(colon + 1));
    } catch (...) {
        return false;
    }

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    // Non-blocking connect with timeout
    int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        ::close(fd);
        return false;
    }

    int ret = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (ret < 0 && errno != EINPROGRESS) {
        ::close(fd);
        return false;
    }

    if (ret < 0) {
        struct pollfd pfd{};
        pfd.fd     = fd;
        pfd.events = POLLOUT;
        int pr = ::poll(&pfd, 1, timeout_ms_);
        if (pr <= 0) {
            ::close(fd);
            return false;
        }
        int sock_err = 0;
        socklen_t len = sizeof(sock_err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &sock_err, &len);
        if (sock_err != 0) {
            ::close(fd);
            return false;
        }
    }

    // Restore blocking and apply timeouts
    ::fcntl(fd, F_SETFL, flags);
    struct timeval tv;
    tv.tv_sec  = timeout_ms_ / 1000;
    tv.tv_usec = (timeout_ms_ % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    // Send PING
    const char* ping_msg = "PING\n";
    ssize_t sent = ::send(fd, ping_msg, 5, 0);
    if (sent != 5) {
        ::close(fd);
        return false;
    }

    // Read response (+PONG\n = 6 bytes)
    char buf[64];
    ssize_t n = ::recv(fd, buf, sizeof(buf) - 1, 0);
    ::close(fd);

    if (n <= 0) return false;
    buf[n] = '\0';
    return std::strncmp(buf, "+PONG", 5) == 0;
}

}  // namespace dkv
