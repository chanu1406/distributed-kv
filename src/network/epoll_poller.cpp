#include "network/poller.h"

#ifdef PLATFORM_LINUX

#include <sys/epoll.h>
#include <unistd.h>
#include <iostream>

namespace dkv {

class EpollPoller : public Poller {
public:
    EpollPoller() {
        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0) {
            std::cerr << "[POLLER] epoll_create1 failed\n";
        }
    }

    ~EpollPoller() override {
        if (epoll_fd_ >= 0) ::close(epoll_fd_);
    }

    bool add_fd(int fd, uint32_t events) override {
        struct epoll_event ev{};
        ev.events = to_epoll_events(events);
        ev.data.fd = fd;
        return epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == 0;
    }

    bool modify_fd(int fd, uint32_t events) override {
        struct epoll_event ev{};
        ev.events = to_epoll_events(events);
        ev.data.fd = fd;
        return epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) == 0;
    }

    bool remove_fd(int fd) override {
        return epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) == 0;
    }

    std::vector<PollEvent> poll(int timeout_ms) override {
        constexpr int MAX_EVENTS = 64;
        struct epoll_event events[MAX_EVENTS];
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, timeout_ms);

        std::vector<PollEvent> result;
        for (int i = 0; i < n; i++) {
            PollEvent pe{};
            pe.fd       = events[i].data.fd;
            pe.readable = (events[i].events & EPOLLIN) != 0;
            pe.writable = (events[i].events & EPOLLOUT) != 0;
            pe.error    = (events[i].events & (EPOLLERR | EPOLLHUP)) != 0;
            result.push_back(pe);
        }
        return result;
    }

private:
    int epoll_fd_ = -1;

    static uint32_t to_epoll_events(uint32_t flags) {
        uint32_t ev = EPOLLET;  // edge-triggered for performance
        if (flags & POLL_READ)  ev |= EPOLLIN;
        if (flags & POLL_WRITE) ev |= EPOLLOUT;
        return ev;
    }
};

std::unique_ptr<Poller> Poller::create() {
    return std::make_unique<EpollPoller>();
}

}  // namespace dkv

#endif  // PLATFORM_LINUX
