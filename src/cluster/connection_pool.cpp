#include "cluster/connection_pool.h"

#include <arpa/inet.h>
#include <netinet/in.h>
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

    apply_timeouts(fd);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        std::cerr << "[POOL] Invalid host: " << host << "\n";
        ::close(fd);
        return -1;
    }

    if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr),
                  sizeof(addr)) < 0) {
        std::cerr << "[POOL] connect() to " << address << " failed: "
                  << strerror(errno) << "\n";
        ::close(fd);
        return -1;
    }

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
