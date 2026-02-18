#include "network/tcp_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>

namespace dkv {

// ── Helpers ──────────────────────────────────────────────────────────────────

namespace {

bool set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

}  // namespace

// ── Constructor / Destructor ─────────────────────────────────────────────────

TCPServer::TCPServer(StorageEngine& engine, uint16_t port, size_t num_workers)
    : engine_(engine), port_(port), pool_(num_workers) {}

TCPServer::~TCPServer() {
    stop();
    if (listen_fd_ >= 0) ::close(listen_fd_);
    if (wakeup_read_fd_ >= 0) ::close(wakeup_read_fd_);
    if (wakeup_write_fd_ >= 0) ::close(wakeup_write_fd_);
    for (auto& [fd, conn] : connections_) {
        ::close(fd);
    }
}

// ── Setup ────────────────────────────────────────────────────────────────────

bool TCPServer::setup_listener() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        std::cerr << "[TCP] socket() failed: " << strerror(errno) << "\n";
        return false;
    }

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    set_nonblocking(listen_fd_);

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port_);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr),
               sizeof(addr)) < 0) {
        std::cerr << "[TCP] bind() failed on port " << port_ << ": "
                  << strerror(errno) << "\n";
        return false;
    }

    if (::listen(listen_fd_, 128) < 0) {
        std::cerr << "[TCP] listen() failed: " << strerror(errno) << "\n";
        return false;
    }

    return true;
}

bool TCPServer::setup_wakeup() {
    int fds[2];
    if (::pipe(fds) < 0) {
        std::cerr << "[TCP] pipe() failed: " << strerror(errno) << "\n";
        return false;
    }
    wakeup_read_fd_  = fds[0];
    wakeup_write_fd_ = fds[1];
    set_nonblocking(wakeup_read_fd_);
    set_nonblocking(wakeup_write_fd_);
    return true;
}

// ── Event Loop ───────────────────────────────────────────────────────────────

void TCPServer::run() {
    poller_ = Poller::create();

    if (!setup_listener()) return;
    if (!setup_wakeup()) return;

    poller_->add_fd(listen_fd_, POLL_READ);
    poller_->add_fd(wakeup_read_fd_, POLL_READ);

    running_ = true;
    std::cout << "[TCP] Listening on port " << port_ << "\n";

    while (running_) {
        auto events = poller_->poll(100);  // 100ms timeout

        for (const auto& ev : events) {
            if (ev.fd == listen_fd_) {
                handle_accept();
            } else if (ev.fd == wakeup_read_fd_) {
                // Drain the wakeup pipe
                char buf[64];
                while (::read(wakeup_read_fd_, buf, sizeof(buf)) > 0) {}
                drain_responses();
            } else {
                if (ev.error) {
                    close_connection(ev.fd);
                    continue;
                }
                if (ev.readable) {
                    handle_read(ev.fd);
                }
                if (ev.writable) {
                    handle_write(ev.fd);
                }
            }
        }
    }
}

void TCPServer::stop() {
    if (!running_.exchange(false)) return;
    // Wake up the event loop so it exits the poll() call
    char c = 1;
    ::write(wakeup_write_fd_, &c, 1);
}

// ── Accept ───────────────────────────────────────────────────────────────────

void TCPServer::handle_accept() {
    // Edge-triggered: accept as many as possible
    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = ::accept(listen_fd_,
                                  reinterpret_cast<struct sockaddr*>(&client_addr),
                                  &addr_len);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            std::cerr << "[TCP] accept() failed: " << strerror(errno) << "\n";
            break;
        }

        set_nonblocking(client_fd);
        poller_->add_fd(client_fd, POLL_READ);
        connections_[client_fd] = Connection{client_fd, "", ""};
    }
}

// ── Read ─────────────────────────────────────────────────────────────────────

void TCPServer::handle_read(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;

    char buf[4096];
    // Edge-triggered: read as much as possible
    while (true) {
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n > 0) {
            it->second.read_buf.append(buf, static_cast<size_t>(n));
        } else if (n == 0) {
            // Client closed connection
            close_connection(fd);
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close_connection(fd);
            return;
        }
    }

    process_commands(fd);
}

// ── Command Processing ──────────────────────────────────────────────────────

void TCPServer::process_commands(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;

    auto& read_buf = it->second.read_buf;

    while (!read_buf.empty()) {
        ParseResult result = try_parse(read_buf.data(), read_buf.size());

        if (result.status == ParseStatus::INCOMPLETE) {
            break;  // wait for more data
        }

        if (result.status == ParseStatus::ERROR) {
            // Send error response, consume the bad frame, and keep going
            std::string resp = format_error(result.error_msg);
            read_buf.erase(0, result.bytes_consumed);

            // Write error directly (small, on event loop thread — acceptable)
            std::lock_guard lock(response_mutex_);
            response_queue_.push_back({fd, std::move(resp)});
            char c = 1;
            ::write(wakeup_write_fd_, &c, 1);
            continue;
        }

        // status == OK — dispatch to worker
        size_t consumed = result.bytes_consumed;
        Command cmd = std::move(result.command);
        read_buf.erase(0, consumed);

        // Capture fd by value for the worker lambda
        pool_.submit([this, fd, cmd = std::move(cmd)]() {
            std::string response = execute_command(cmd);

            // Push response to queue and wake event loop
            {
                std::lock_guard lock(response_mutex_);
                response_queue_.push_back({fd, std::move(response)});
            }
            char c = 1;
            ::write(wakeup_write_fd_, &c, 1);
        });
    }
}

std::string TCPServer::execute_command(const Command& cmd) {
    auto now = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );

    switch (cmd.type) {
        case CommandType::PING:
            return format_pong();

        case CommandType::GET: {
            auto result = engine_.get(cmd.key);
            if (!result.found) return format_not_found();
            return format_value(result.value);
        }

        case CommandType::SET: {
            Version v{now, 1};  // node_id=1 for now (will come from config in Phase 4)
            engine_.set(cmd.key, cmd.value, v);
            return format_ok();
        }

        case CommandType::DEL: {
            Version v{now, 1};
            engine_.del(cmd.key, v);
            return format_ok();
        }
    }
    return format_error("INTERNAL");
}

// ── Write ────────────────────────────────────────────────────────────────────

void TCPServer::drain_responses() {
    std::vector<PendingResponse> batch;
    {
        std::lock_guard lock(response_mutex_);
        batch.swap(response_queue_);
    }

    for (auto& resp : batch) {
        auto it = connections_.find(resp.fd);
        if (it == connections_.end()) continue;  // connection closed

        it->second.write_buf.append(resp.data);

        // Try to write immediately
        handle_write(resp.fd);
    }
}

void TCPServer::handle_write(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;

    auto& write_buf = it->second.write_buf;

    while (!write_buf.empty()) {
        ssize_t n = ::write(fd, write_buf.data(), write_buf.size());
        if (n > 0) {
            write_buf.erase(0, static_cast<size_t>(n));
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Register for write readiness and try later
                poller_->modify_fd(fd, POLL_READ | POLL_WRITE);
                return;
            }
            close_connection(fd);
            return;
        }
    }

    // All data written — stop monitoring for write readiness
    poller_->modify_fd(fd, POLL_READ);
}

// ── Cleanup ──────────────────────────────────────────────────────────────────

void TCPServer::close_connection(int fd) {
    poller_->remove_fd(fd);
    ::close(fd);
    connections_.erase(fd);
}

}  // namespace dkv
