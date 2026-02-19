#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace dkv {

/// A connection acquired from the pool.
struct PooledConnection {
    int         fd = -1;
    std::string address;     // "host:port" of the peer
};

/// Pool of persistent TCP connections to peer nodes.
///
/// Thread-safe: multiple workers can acquire/release connections
/// concurrently.  Connections are reused across requests to avoid
/// the overhead of TCP handshake per proxied request.
class ConnectionPool {
public:
    /// @param max_per_peer  Maximum idle connections kept per peer address.
    /// @param timeout_ms    SO_RCVTIMEO / SO_SNDTIMEO applied to new sockets.
    explicit ConnectionPool(size_t max_per_peer = 4, int timeout_ms = 500);

    /// Get a connection to the given address ("host:port").
    /// Reuses an idle connection if available, otherwise creates a new one.
    /// Returns std::nullopt if the connection cannot be established.
    std::optional<PooledConnection> acquire(const std::string& address);

    /// Return a connection to the pool for reuse.
    /// If the pool for this peer is full, the connection is closed instead.
    void release(PooledConnection conn);

    /// Close all pooled connections (e.g., during shutdown).
    void close_all();

    ~ConnectionPool();

    // Non-copyable
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

private:
    size_t max_per_peer_;
    int    timeout_ms_;

    std::mutex mutex_;
    std::unordered_map<std::string, std::vector<int>> pools_;  // address → idle fds

    /// Create a new TCP connection to the given address.
    /// Returns the fd, or -1 on failure.
    int connect_to(const std::string& address);

    /// Apply socket timeouts to a file descriptor.
    void apply_timeouts(int fd);
};

}  // namespace dkv
