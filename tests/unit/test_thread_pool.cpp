#include <gtest/gtest.h>

#include "network/thread_pool.h"

#include <atomic>
#include <chrono>

TEST(ThreadPool, SubmitAndExecute) {
    dkv::ThreadPool pool(2);
    std::atomic<int> counter{0};

    pool.submit([&]() { counter++; });
    pool.submit([&]() { counter++; });
    pool.submit([&]() { counter++; });

    pool.shutdown();
    EXPECT_EQ(counter.load(), 3);
}

TEST(ThreadPool, ConcurrentSubmit) {
    dkv::ThreadPool pool(4);
    std::atomic<int> counter{0};
    constexpr int TASKS_PER_THREAD = 100;
    constexpr int NUM_SUBMITTERS = 4;

    // Spawn multiple threads that each submit many tasks
    std::vector<std::thread> submitters;
    for (int t = 0; t < NUM_SUBMITTERS; t++) {
        submitters.emplace_back([&]() {
            for (int i = 0; i < TASKS_PER_THREAD; i++) {
                pool.submit([&]() { counter++; });
            }
        });
    }

    for (auto& s : submitters) {
        s.join();
    }
    pool.shutdown();

    EXPECT_EQ(counter.load(), TASKS_PER_THREAD * NUM_SUBMITTERS);
}

TEST(ThreadPool, ShutdownRejectsNewTasks) {
    dkv::ThreadPool pool(2);
    pool.shutdown();

    // After shutdown, submit should return false
    bool accepted = pool.submit([]() {});
    EXPECT_FALSE(accepted);
}

TEST(ThreadPool, DestructorCallsShutdown) {
    std::atomic<int> counter{0};
    {
        dkv::ThreadPool pool(2);
        pool.submit([&]() { counter++; });
    }
    // If destructor didn't join, this could crash or be racy
    EXPECT_EQ(counter.load(), 1);
}

TEST(ThreadPool, TasksRunOnDifferentThreads) {
    dkv::ThreadPool pool(4);
    std::mutex ids_mutex;
    std::set<std::thread::id> thread_ids;

    for (int i = 0; i < 20; i++) {
        pool.submit([&]() {
            std::lock_guard lock(ids_mutex);
            thread_ids.insert(std::this_thread::get_id());
            // Small delay to make it more likely different threads pick up tasks
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        });
    }

    pool.shutdown();
    // We should see more than 1 thread if the pool is working correctly
    EXPECT_GT(thread_ids.size(), 1u);
}
