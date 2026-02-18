#pragma once

#include "network/poller.h"
#include "network/protocol.h"
#include "network/thread_pool.h"
#include "storage/storage_engine.h"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace dkv {

/// Per-connection state, owned exclusively by the event loop thread.
struct Connection {
    int         fd = -1;
    std::string read_buf;    // accumulated incoming bytes
    std::string write_buf;   // pending outgoing bytes
};

/// Response from a worker thread, to be written back on the event loop.
struct PendingResponse {
    int         fd;
    std::string data;
};

/// Reactor-pattern TCP server.
///
/// Ownership rules (per ChatGPT feedback §3):
///   - The event loop thread owns all Connection state and socket I/O.
///   - Workers receive parsed Commands and return response strings.
///   - Responses are pushed back via a thread-safe queue + wakeup fd.
class TCPServer {
public:
    /// Construct the server.
    /// @param engine   Reference to the storage engine (must outlive the server).
    /// @param port     TCP port to listen on.
    /// @param num_workers  Number of worker threads.
    TCPServer(StorageEngine& engine, uint16_t port, size_t num_workers);

    /// Start the event loop.  Blocks the calling thread until stop() is called.
    void run();

    /// Signal the event loop to stop (thread-safe).
    void stop();

    ~TCPServer();

    // Non-copyable
    TCPServer(const TCPServer&) = delete;
    TCPServer& operator=(const TCPServer&) = delete;

private:
    StorageEngine&                           engine_;
    uint16_t                                 port_;
    std::unique_ptr<Poller>                  poller_;
    ThreadPool                               pool_;
    int                                      listen_fd_ = -1;
    int                                      wakeup_read_fd_ = -1;
    int                                      wakeup_write_fd_ = -1;
    std::atomic<bool>                        running_{false};

    // Connections owned by the event loop thread
    std::unordered_map<int, Connection>      connections_;

    // Thread-safe response queue (workers → event loop)
    std::mutex                               response_mutex_;
    std::vector<PendingResponse>             response_queue_;

    /// Create a non-blocking, SO_REUSEADDR listening socket.
    bool setup_listener();

    /// Create the wakeup pipe (eventfd on Linux, pipe on macOS).
    bool setup_wakeup();

    /// Accept new connections from the listen socket.
    void handle_accept();

    /// Read data from a client connection.
    void handle_read(int fd);

    /// Try to parse and dispatch commands from the connection's read buffer.
    void process_commands(int fd);

    /// Execute a parsed command on the storage engine.
    std::string execute_command(const Command& cmd);

    /// Write queued data to a connection's socket.
    void handle_write(int fd);

    /// Drain the response queue (called from event loop after wakeup).
    void drain_responses();

    /// Close and clean up a connection.
    void close_connection(int fd);
};

}  // namespace dkv
