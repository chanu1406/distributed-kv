#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace dkv {

/// A fixed-size thread pool with a blocking task queue.
///
/// Ownership rules (per ChatGPT feedback §3):
///   - The event loop thread submits tasks (parsed requests).
///   - Workers execute business logic and return response objects.
///   - Workers NEVER touch socket I/O or connection state.
class ThreadPool {
public:
    /// Create the pool with `num_threads` workers and start them immediately.
    explicit ThreadPool(size_t num_threads);

    /// Submit a task for asynchronous execution.  Returns false if the pool
    /// has been shut down.
    bool submit(std::function<void()> task);

    /// Signal all workers to stop after finishing their current task.
    /// Blocks until all threads have joined.  Pending tasks in the queue
    /// are discarded.
    void shutdown();

    /// Returns the number of worker threads.
    size_t size() const { return workers_.size(); }

    ~ThreadPool();

    // Non-copyable, non-movable
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopped_ = false;
};

}  // namespace dkv
