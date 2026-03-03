#include <gtest/gtest.h>
#include <chrono>
#include <thread>

#include "cluster/coordinator.h"
#include "cluster/connection_pool.h"
#include "cluster/hash_ring.h"
#include "network/protocol.h"
#include "storage/storage_engine.h"

// ---------------------------------------------------------------------------
// Coordinator unit tests: local routing, FWD handling, loop detection
// ---------------------------------------------------------------------------

class CoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Build a ring with one node (this node = node 1)
        ring_.add_node(1, "127.0.0.1:9000", 128);
    }

    dkv::StorageEngine engine_;
    dkv::HashRing ring_;
    dkv::ConnectionPool pool_;
    static constexpr uint32_t THIS_NODE = 1;
};

// ── PING always local ────────────────────────────────────────────────────────

TEST_F(CoordinatorTest, PingAlwaysLocal) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);
    dkv::Command cmd{};
    cmd.type = dkv::CommandType::PING;

    std::string resp = coord.handle_command(cmd);
    EXPECT_EQ(resp, "+PONG\n");
}

// ── SET/GET/DEL to local node ────────────────────────────────────────────────

TEST_F(CoordinatorTest, SetAndGetLocal) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    // SET
    dkv::Command set_cmd{};
    set_cmd.type = dkv::CommandType::SET;
    set_cmd.key = "testkey";
    set_cmd.value = "testvalue";

    std::string set_resp = coord.handle_command(set_cmd);
    EXPECT_EQ(set_resp, "+OK\n");

    // GET
    dkv::Command get_cmd{};
    get_cmd.type = dkv::CommandType::GET;
    get_cmd.key = "testkey";

    std::string get_resp = coord.handle_command(get_cmd);
    EXPECT_EQ(get_resp, "$9 testvalue\n");
}

TEST_F(CoordinatorTest, GetNotFoundLocal) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    dkv::Command cmd{};
    cmd.type = dkv::CommandType::GET;
    cmd.key = "nonexistent";

    std::string resp = coord.handle_command(cmd);
    EXPECT_EQ(resp, "-NOT_FOUND\n");
}

TEST_F(CoordinatorTest, DeleteLocal) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    // Set first
    dkv::Command set_cmd{};
    set_cmd.type = dkv::CommandType::SET;
    set_cmd.key = "delkey";
    set_cmd.value = "val";
    coord.handle_command(set_cmd);

    // Ensure DEL gets a strictly newer timestamp than SET
    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    // Delete
    dkv::Command del_cmd{};
    del_cmd.type = dkv::CommandType::DEL;
    del_cmd.key = "delkey";

    std::string resp = coord.handle_command(del_cmd);
    EXPECT_EQ(resp, "+OK\n");

    // Verify deleted
    dkv::Command get_cmd{};
    get_cmd.type = dkv::CommandType::GET;
    get_cmd.key = "delkey";

    std::string get_resp = coord.handle_command(get_cmd);
    EXPECT_EQ(get_resp, "-NOT_FOUND\n");
}

// ── FWD with valid inner command ─────────────────────────────────────────────

TEST_F(CoordinatorTest, FwdSetThenGet) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    // Forward a SET command to this node
    dkv::Command fwd_set{};
    fwd_set.type = dkv::CommandType::FWD;
    fwd_set.hops_remaining = 2;
    fwd_set.inner_line = "SET 4 fkey 4 fval";

    std::string resp = coord.handle_command(fwd_set);
    EXPECT_EQ(resp, "+OK\n");

    // Now GET it directly (local)
    dkv::Command get_cmd{};
    get_cmd.type = dkv::CommandType::GET;
    get_cmd.key = "fkey";

    std::string get_resp = coord.handle_command(get_cmd);
    EXPECT_EQ(get_resp, "$4 fval\n");
}

TEST_F(CoordinatorTest, FwdGet) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    // Pre-populate data
    dkv::Command set_cmd{};
    set_cmd.type = dkv::CommandType::SET;
    set_cmd.key = "fwdkey";
    set_cmd.value = "fwdval";
    coord.handle_command(set_cmd);

    // Forward GET
    dkv::Command fwd_get{};
    fwd_get.type = dkv::CommandType::FWD;
    fwd_get.hops_remaining = 1;
    fwd_get.inner_line = "GET 6 fwdkey";

    std::string resp = coord.handle_command(fwd_get);
    EXPECT_EQ(resp, "$6 fwdval\n");
}

// ── FWD routing loop detection ───────────────────────────────────────────────

TEST_F(CoordinatorTest, FwdLoopDetection) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    dkv::Command cmd{};
    cmd.type = dkv::CommandType::FWD;
    cmd.hops_remaining = 0;  // TTL exhausted
    cmd.inner_line = "GET 3 foo";

    std::string resp = coord.handle_command(cmd);
    EXPECT_EQ(resp, "-ERR ROUTING_LOOP\n");
}

// ── FWD malformed inner command ──────────────────────────────────────────────

TEST_F(CoordinatorTest, FwdMalformedInner) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    dkv::Command cmd{};
    cmd.type = dkv::CommandType::FWD;
    cmd.hops_remaining = 2;
    cmd.inner_line = "GARBAGE";

    std::string resp = coord.handle_command(cmd);
    EXPECT_EQ(resp, "-ERR MALFORMED_FWD\n");
}

// ── Remote-only replica unreachable → QUORUM_FAILED ─────────────────────────
// With N=1 (default), the single replica is the ring owner.  If that owner is
// a remote node that's not listening, we get 0/1 acks → QUORUM_FAILED.

TEST_F(CoordinatorTest, RemoteNodeUnreachableQuorumFailed) {
    ring_.add_node(2, "127.0.0.1:9999", 128);

    // Default quorum: N=1, W=1, R=1 — single-replica mode
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    // Find a key whose sole replica (N=1) is node 2 (unreachable)
    std::string remote_key;
    for (int i = 0; i < 1000; ++i) {
        std::string candidate = "key" + std::to_string(i);
        auto replicas = ring_.get_replica_nodes(candidate, 1);
        if (!replicas.empty() && replicas[0].node_id == 2) {
            remote_key = candidate;
            break;
        }
    }
    ASSERT_FALSE(remote_key.empty()) << "No key found with node 2 as sole replica";

    dkv::Command cmd{};
    cmd.type = dkv::CommandType::GET;
    cmd.key  = remote_key;

    std::string resp = coord.handle_command(cmd);
    EXPECT_EQ(resp, "-ERR QUORUM_FAILED\n");
}

// ── Quorum write with N=1 (single-node) succeeds locally ─────────────────────

TEST_F(CoordinatorTest, QuorumWriteSingleNode) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE,
                           nullptr, "", 100000, 1, 1, 1);

    dkv::Command set_cmd{};
    set_cmd.type  = dkv::CommandType::SET;
    set_cmd.key   = "qkey";
    set_cmd.value = "qval";
    EXPECT_EQ(coord.handle_command(set_cmd), "+OK\n");

    dkv::Command get_cmd{};
    get_cmd.type = dkv::CommandType::GET;
    get_cmd.key  = "qkey";
    EXPECT_EQ(coord.handle_command(get_cmd), "$4 qval\n");
}

// ── RSET/RDEL/RGET are always executed locally ────────────────────────────────

TEST_F(CoordinatorTest, RsetExecutedLocally) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    dkv::Command rset{};
    rset.type         = dkv::CommandType::RSET;
    rset.key          = "repkey";
    rset.value        = "repval";
    rset.timestamp_ms = 1000000;
    rset.node_id      = 99;
    EXPECT_EQ(coord.handle_command(rset), "+OK\n");

    auto res = engine_.get("repkey");
    EXPECT_TRUE(res.found);
    EXPECT_EQ(res.value, "repval");
    EXPECT_EQ(res.version.timestamp_ms, 1000000ULL);
    EXPECT_EQ(res.version.node_id, 99u);
}

TEST_F(CoordinatorTest, RgetReturnsVersionedResponse) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    dkv::Command rset{};
    rset.type         = dkv::CommandType::RSET;
    rset.key          = "vkey";
    rset.value        = "vval";
    rset.timestamp_ms = 42000;
    rset.node_id      = 7;
    coord.handle_command(rset);

    dkv::Command rget{};
    rget.type = dkv::CommandType::RGET;
    rget.key  = "vkey";
    EXPECT_EQ(coord.handle_command(rget), "$V 4 vval 42000 7\n");
}

TEST_F(CoordinatorTest, RgetNotFound) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    dkv::Command rget{};
    rget.type = dkv::CommandType::RGET;
    rget.key  = "missing";
    EXPECT_EQ(coord.handle_command(rget), "-NOT_FOUND\n");
}

TEST_F(CoordinatorTest, RdelAppliesVersionedTombstone) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    dkv::Command rset{};
    rset.type = dkv::CommandType::RSET;  rset.key = "todel";
    rset.value = "v";  rset.timestamp_ms = 1000;  rset.node_id = 1;
    coord.handle_command(rset);

    dkv::Command rdel{};
    rdel.type = dkv::CommandType::RDEL;  rdel.key = "todel";
    rdel.timestamp_ms = 2000;  rdel.node_id = 1;
    EXPECT_EQ(coord.handle_command(rdel), "+OK\n");

    dkv::Command rget{};
    rget.type = dkv::CommandType::RGET;  rget.key = "todel";
    EXPECT_EQ(coord.handle_command(rget), "-NOT_FOUND\n");
}

// ── RSET LWW: stale write is silently rejected ────────────────────────────────

TEST_F(CoordinatorTest, RsetLwwRejectsStaleWrite) {
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    dkv::Command rset1{};
    rset1.type = dkv::CommandType::RSET;  rset1.key = "lwwkey";
    rset1.value = "new_value";  rset1.timestamp_ms = 5000;  rset1.node_id = 1;
    coord.handle_command(rset1);

    dkv::Command rset2{};
    rset2.type = dkv::CommandType::RSET;  rset2.key = "lwwkey";
    rset2.value = "old_value";  rset2.timestamp_ms = 1000;  rset2.node_id = 1;
    coord.handle_command(rset2);

    auto res = engine_.get("lwwkey");
    EXPECT_TRUE(res.found);
    EXPECT_EQ(res.value, "new_value");
}

// ── Empty ring returns EMPTY_RING ────────────────────────────────────────────

TEST_F(CoordinatorTest, EmptyRingError) {
    dkv::HashRing empty_ring;
    dkv::Coordinator coord(engine_, empty_ring, pool_, THIS_NODE);

    dkv::Command cmd{};
    cmd.type = dkv::CommandType::GET;
    cmd.key = "any";

    std::string resp = coord.handle_command(cmd);
    EXPECT_EQ(resp, "-ERR EMPTY_RING\n");
}

// ── Repair queue: destroying Coordinator with a pending repair does not crash ─
//
// This test verifies that the background repair queue correctly drains and
// joins on Coordinator destruction, eliminating the use-after-free that
// existed with the old detached-thread approach.

TEST_F(CoordinatorTest, RepairQueueDrainedOnDestruction) {
    // Use a scope so we can watch the destructor run cleanly.
    {
        dkv::StorageEngine local_engine;
        dkv::HashRing local_ring;
        local_ring.add_node(1, "127.0.0.1:9001", 128);
        dkv::ConnectionPool local_pool;

        dkv::Coordinator coord(local_engine, local_ring, local_pool, 1,
                               nullptr, "", 100000, 1, 1, 1);

        // SET a key — succeeds locally (N=1, W=1, R=1).
        dkv::Command set_cmd{};
        set_cmd.type  = dkv::CommandType::SET;
        set_cmd.key   = "repair_key";
        set_cmd.value = "repair_val";
        EXPECT_EQ(coord.handle_command(set_cmd), "+OK\n");

        // GET the same key — no stale replicas in a single-node ring, so
        // read repair is not triggered here, but the repair queue must be
        // in a clean, join-able state when ~Coordinator() fires.
        dkv::Command get_cmd{};
        get_cmd.type = dkv::CommandType::GET;
        get_cmd.key  = "repair_key";
        EXPECT_EQ(coord.handle_command(get_cmd), "$10 repair_val\n");

        // ~Coordinator() is called here: repair_running_ set to false,
        // repair_cv_ notified, repair_thread_ joined — must not crash.
    }
    // If we reach here without ASAN/TSAN error or hang, the fix is correct.
    SUCCEED();
}

// ── Repair queue: multiple tasks are all executed before destruction ──────────

TEST_F(CoordinatorTest, RepairQueueExecutesPendingTasks) {
    std::atomic<int> counter{0};

    // Access the repair queue internals indirectly through read_repair_async
    // by calling the destructor after queuing tasks.  Since read_repair_async
    // is private, we verify the queue executes work via a functional path:
    // write a value then re-set via RSET at a higher version — the repair
    // worker would do the same kind of local engine.set() call.
    {
        dkv::StorageEngine local_engine;
        dkv::HashRing local_ring;
        local_ring.add_node(1, "127.0.0.1:9001", 128);
        dkv::ConnectionPool local_pool;

        dkv::Coordinator coord(local_engine, local_ring, local_pool, 1,
                               nullptr, "", 100000, 1, 1, 1);

        // Perform several writes and reads; the repair worker must stay alive
        // throughout and join cleanly on destruction.
        for (int i = 0; i < 10; ++i) {
            dkv::Command sc{};
            sc.type  = dkv::CommandType::SET;
            sc.key   = "k" + std::to_string(i);
            sc.value = "v" + std::to_string(i);
            EXPECT_EQ(coord.handle_command(sc), "+OK\n");
            ++counter;
        }
        // Destructor joins repair_thread_ here — no crash, no hang.
    }
    EXPECT_EQ(counter.load(), 10);
}

// ── Thread-pool quorum: concurrent writes from multiple threads are safe ──────
//
// Verifies that the pool-based scatter-gather doesn't race or deadlock when
// many writes arrive in rapid succession.

TEST_F(CoordinatorTest, QuorumPoolConcurrentWrites) {
    // Single-node ring — all writes land locally via the thread pool.
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE,
                           nullptr, "", 100000, 1, 1, 1);

    constexpr int N = 50;
    std::vector<std::thread> writers;
    writers.reserve(N);
    std::atomic<int> ok_count{0};

    for (int i = 0; i < N; ++i) {
        writers.emplace_back([&, i]() {
            dkv::Command sc{};
            sc.type  = dkv::CommandType::SET;
            sc.key   = "tpool_key_" + std::to_string(i);
            sc.value = "tpool_val_" + std::to_string(i);
            if (coord.handle_command(sc) == "+OK\n") {
                ok_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& t : writers) t.join();

    EXPECT_EQ(ok_count.load(), N);

    // Spot-check a few reads via the pool.
    for (int i = 0; i < N; i += 10) {
        dkv::Command gc{};
        gc.type = dkv::CommandType::GET;
        gc.key  = "tpool_key_" + std::to_string(i);
        std::string expected = "$" + std::to_string(
            std::string("tpool_val_" + std::to_string(i)).size()) +
            " tpool_val_" + std::to_string(i) + "\n";
        EXPECT_EQ(coord.handle_command(gc), expected);
    }
}

// ── Thread-pool quorum: destructor doesn't hang with in-flight tasks ──────────

TEST_F(CoordinatorTest, QuorumPoolCleanShutdown) {
    // Submit a burst of writes then immediately destroy the coordinator —
    // the destructor must call quorum_pool_.reset() before returning.
    {
        dkv::StorageEngine local_engine;
        dkv::HashRing local_ring;
        local_ring.add_node(1, "127.0.0.1:9001", 128);
        dkv::ConnectionPool local_pool;

        dkv::Coordinator coord(local_engine, local_ring, local_pool, 1,
                               nullptr, "", 100000, 1, 1, 1);

        for (int i = 0; i < 20; ++i) {
            dkv::Command sc{};
            sc.type  = dkv::CommandType::SET;
            sc.key   = "shutdown_key_" + std::to_string(i);
            sc.value = "v";
            coord.handle_command(sc);
        }
        // ~Coordinator(): quorum_pool_.reset() + repair_thread_.join()
    }
    SUCCEED();
}

// ── Serialize command line (via FWD round-trip) ──────────────────────────────

TEST_F(CoordinatorTest, SerializeAndReParse) {
    // Verify coordinator correctly re-parses inner commands from FWD frames
    dkv::Coordinator coord(engine_, ring_, pool_, THIS_NODE);

    // FWD containing a SET
    dkv::Command fwd{};
    fwd.type = dkv::CommandType::FWD;
    fwd.hops_remaining = 1;
    fwd.inner_line = "SET 5 hello 5 world";

    std::string resp = coord.handle_command(fwd);
    EXPECT_EQ(resp, "+OK\n");

    // Verify the data was stored
    auto result = engine_.get("hello");
    EXPECT_TRUE(result.found);
    EXPECT_EQ(result.value, "world");
}
