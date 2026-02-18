#include "network/poller.h"

#ifdef PLATFORM_MACOS

#include <sys/event.h>
#include <unistd.h>
#include <iostream>

namespace dkv {

class KqueuePoller : public Poller {
public:
    KqueuePoller() {
        kq_fd_ = kqueue();
        if (kq_fd_ < 0) {
            std::cerr << "[POLLER] kqueue() failed\n";
        }
    }

    ~KqueuePoller() override {
        if (kq_fd_ >= 0) ::close(kq_fd_);
    }

    bool add_fd(int fd, uint32_t events) override {
        return apply_changes(fd, events, EV_ADD | EV_CLEAR);
    }

    bool modify_fd(int fd, uint32_t events) override {
        // kqueue doesn't have a modify — just re-add with EV_ADD
        return apply_changes(fd, events, EV_ADD | EV_CLEAR);
    }

    bool remove_fd(int fd) override {
        struct kevent changes[2];
        int n = 0;
        EV_SET(&changes[n++], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        // Ignore errors — the filter may not have been registered
        kevent(kq_fd_, changes, n, nullptr, 0, nullptr);
        return true;
    }

    std::vector<PollEvent> poll(int timeout_ms) override {
        constexpr int MAX_EVENTS = 64;
        struct kevent events[MAX_EVENTS];

        struct timespec ts;
        struct timespec* ts_ptr = nullptr;
        if (timeout_ms >= 0) {
            ts.tv_sec  = timeout_ms / 1000;
            ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
            ts_ptr     = &ts;
        }

        int n = kevent(kq_fd_, nullptr, 0, events, MAX_EVENTS, ts_ptr);

        // Merge read/write events for the same fd
        std::vector<PollEvent> result;
        for (int i = 0; i < n; i++) {
            int fd = static_cast<int>(events[i].ident);

            // Find or create entry for this fd
            PollEvent* pe = nullptr;
            for (auto& existing : result) {
                if (existing.fd == fd) { pe = &existing; break; }
            }
            if (!pe) {
                result.push_back({fd, false, false, false});
                pe = &result.back();
            }

            if (events[i].filter == EVFILT_READ)  pe->readable = true;
            if (events[i].filter == EVFILT_WRITE) pe->writable = true;
            if (events[i].flags & EV_EOF || events[i].flags & EV_ERROR) pe->error = true;
        }
        return result;
    }

private:
    int kq_fd_ = -1;

    bool apply_changes(int fd, uint32_t flags, uint16_t ev_flags) {
        struct kevent changes[2];
        int n = 0;

        if (flags & POLL_READ) {
            EV_SET(&changes[n++], fd, EVFILT_READ, ev_flags, 0, 0, nullptr);
        } else {
            EV_SET(&changes[n++], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        }

        if (flags & POLL_WRITE) {
            EV_SET(&changes[n++], fd, EVFILT_WRITE, ev_flags, 0, 0, nullptr);
        } else {
            EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        }

        int result = kevent(kq_fd_, changes, n, nullptr, 0, nullptr);
        // Tolerate EV_DELETE failures for filters not yet registered
        return result >= 0 || (flags != 0);
    }
};

std::unique_ptr<Poller> Poller::create() {
    return std::make_unique<KqueuePoller>();
}

}  // namespace dkv

#endif  // PLATFORM_MACOS
