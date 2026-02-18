#pragma once

#include <memory>
#include <vector>
#include <cstdint>

namespace dkv {

/// Events of interest for polling.
enum PollFlags : uint32_t {
    POLL_READ  = 1u << 0,
    POLL_WRITE = 1u << 1,
};

/// Result of a single poll event.
struct PollEvent {
    int      fd;
    bool     readable;
    bool     writable;
    bool     error;     // HUP, ERR, etc.
};

/// Abstract I/O multiplexer.  Implemented by EpollPoller (Linux) and
/// KqueuePoller (macOS).
class Poller {
public:
    virtual ~Poller() = default;

    /// Register a file descriptor for the given events.
    virtual bool add_fd(int fd, uint32_t events) = 0;

    /// Change the events a file descriptor is monitored for.
    virtual bool modify_fd(int fd, uint32_t events) = 0;

    /// Remove a file descriptor from the poll set.
    virtual bool remove_fd(int fd) = 0;

    /// Block up to `timeout_ms` waiting for events.  Returns ready fds.
    /// Pass -1 for infinite timeout.
    virtual std::vector<PollEvent> poll(int timeout_ms) = 0;

    /// Factory: creates the platform-appropriate poller.
    static std::unique_ptr<Poller> create();
};

}  // namespace dkv
