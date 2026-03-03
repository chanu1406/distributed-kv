#include "cluster/connection_pool.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

namespace dkv {

ConnectionPool::ConnectionPool(size_t max_per_peer, int timeout_ms)
    : max_per_peer_(max_per_peer), timeout_ms_(timeout_ms) {}

ConnectionPool::~ConnectionPool() {
    close_all();
}

std::optional<PooledConnection> ConnectionPool::acquire(const std::string& address) {
    {
        std::lock_guard lock(mutex_);
        auto it = pools_.find(address);
        if (it != pools_.end() && !it->second.empty()) {
            int fd = it->second.back();
            it->second.pop_back();
            return PooledConnection{fd, address};
        }
    }

    // No idle connection — create a new one
    int fd = connect_to(address);
    if (fd < 0) return std::nullopt;

    return PooledConnection{fd, address};
}

void ConnectionPool::release(PooledConnection conn) {
    std::lock_guard lock(mutex_);
    auto& pool = pools_[conn.address];

    if (pool.size() < max_per_peer_) {
        pool.push_back(conn.fd);
    } else {
        // Pool is full — close the extra connection
        ::close(conn.fd);
    }
}

void ConnectionPool::close_all() {
    std::lock_guard lock(mutex_);
    for (auto& [addr, fds] : pools_) {
        for (int fd : fds) {
            ::close(fd);
        }
    }
    pools_.clear();
}

int ConnectionPool::connect_to(const std::string& address) {
    // Parse "host:port"
    auto colon = address.rfind(':');
    if (colon == std::string::npos) {
        std::cerr << "[POOL] Invalid address: " << address << "\n";
        return -1;
    }

    std::string host = address.substr(0, colon);
    int port = 0;
    try {
        port = std::stoi(address.substr(colon + 1));
    } catch (...) {
        std::cerr << "[POOL] Invalid port in: " << address << "\n";
        return -1;
    }

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "[POOL] socket() failed: " << strerror(errno) << "\n";
        return -1;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        std::cerr << "[POOL] Invalid host: " << host << "\n";
        ::close(fd);
        return -1;
    }

    // Set non-blocking for connect so the OS TCP handshake timeout (~60-120s)
    // doesn't block the quorum thread; we enforce our own timeout_ms_ instead.
    int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    int ret = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr),
                        sizeof(addr));
    if (ret < 0 && errno != EINPROGRESS) {
        std::cerr << "[POOL] connect() to " << address << " failed: "
                  << strerror(errno) << "\n";
        ::close(fd);
        return -1;
    }

    if (ret < 0) {
        // EINPROGRESS: wait for the connection to complete within timeout_ms_.
        struct pollfd pfd{};
        pfd.fd     = fd;
        pfd.events = POLLOUT;

        int poll_ret = ::poll(&pfd, 1, timeout_ms_);
        if (poll_ret <= 0) {
            // Timeout (0) or poll error (<0)
            std::cerr << "[POOL] connect() to " << address
                      << (poll_ret == 0 ? " timed out" : " poll error") << "\n";
            ::close(fd);
            return -1;
        }

        // POLLOUT fired — check whether the connection actually succeeded.
        int sock_err = 0;
        socklen_t len = sizeof(sock_err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &sock_err, &len);
        if (sock_err != 0) {
            std::cerr << "[POOL] connect() to " << address << " failed: "
                      << strerror(sock_err) << "\n";
            ::close(fd);
            return -1;
        }
    }

    // Restore blocking mode for normal send/recv I/O.
    ::fcntl(fd, F_SETFL, flags);

    // Apply send/recv timeouts (these do not affect connect).
    apply_timeouts(fd);

    return fd;
}

void ConnectionPool::apply_timeouts(int fd) {
    struct timeval tv;
    tv.tv_sec  = timeout_ms_ / 1000;
    tv.tv_usec = (timeout_ms_ % 1000) * 1000;

    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

}  // namespace dkv
