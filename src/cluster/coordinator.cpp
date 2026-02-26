#include "cluster/coordinator.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <unistd.h>

namespace dkv {

namespace {
/// Current time in milliseconds since epoch.
inline uint64_t now_ms() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count());
}
}  // namespace

Coordinator::Coordinator(StorageEngine& engine, HashRing& ring,
                         ConnectionPool& pool, uint32_t node_id,
                         WAL* wal, const std::string& snapshot_dir,
                         uint64_t snapshot_interval,
                         uint32_t replication_factor,
                         uint32_t write_quorum,
                         uint32_t read_quorum,
                         const std::string& hints_dir)
    : engine_(engine), ring_(ring), pool_(pool), node_id_(node_id),
      wal_(wal), snapshot_dir_(snapshot_dir),
      snapshot_interval_(snapshot_interval),
      replication_factor_(replication_factor),
      write_quorum_(write_quorum),
      read_quorum_(read_quorum),
      hints_(hints_dir) {
    // Recover any hints persisted before a previous coordinator crash.
    hints_.load();
}

std::string Coordinator::handle_command(const Command& cmd) {
    // PING is always handled locally
    if (cmd.type == CommandType::PING) {
        return format_pong();
    }

    // FWD: decrement hop counter, then re-parse and execute the inner command
    // locally (we are the target node for this forwarded request).
    if (cmd.type == CommandType::FWD) {
        if (cmd.hops_remaining == 0) {
            return format_error("ROUTING_LOOP");
        }

        std::string inner_with_nl = cmd.inner_line + "\n";
        ParseResult inner_result = try_parse(inner_with_nl.data(),
                                              inner_with_nl.size());
        if (inner_result.status != ParseStatus::OK) {
            return format_error("MALFORMED_FWD");
        }

        return execute_local(inner_result.command);
    }

    // RSET/RDEL/RGET are internal replication commands sent by the quorum
    // coordinator directly to this node.  Always execute locally — no further
    // routing needed; the coordinator already selected us as a replica.
    if (cmd.type == CommandType::RSET ||
        cmd.type == CommandType::RDEL ||
        cmd.type == CommandType::RGET) {
        return execute_local(cmd);
    }

    // Client SET/DEL: scatter to N replicas, wait for W acks (§9.B).
    if (cmd.type == CommandType::SET || cmd.type == CommandType::DEL) {
        return quorum_write(cmd.key, cmd.value,
                            cmd.type == CommandType::DEL);
    }

    // Client GET: query R replicas, return highest-version value (§9.C).
    if (cmd.type == CommandType::GET) {
        return quorum_read(cmd.key);
    }

    return format_error("INTERNAL");
}

std::string Coordinator::execute_local(const Command& cmd) {
    const uint64_t ts = now_ms();

    switch (cmd.type) {
        case CommandType::PING:
            return format_pong();

        // ── Client GET (used via FWD inner command) ──────────────────────────
        case CommandType::GET: {
            auto result = engine_.get(cmd.key);
            if (!result.found) return format_not_found();
            return format_value(result.value);
        }

        // ── Client SET/DEL (used via FWD inner command) ──────────────────────
        // Note: when a client SET/DEL arrives directly (not via FWD), it goes
        // through handle_command → quorum_write/quorum_read instead.
        case CommandType::SET: {
            if (wal_) {
                WalRecord rec;
                rec.timestamp_ms = ts;
                rec.op_type      = OpType::SET;
                rec.key           = cmd.key;
                rec.value         = cmd.value;
                wal_->append(rec);
            }
            Version v{ts, node_id_};
            engine_.set(cmd.key, cmd.value, v);
            maybe_snapshot();
            return format_ok();
        }

        case CommandType::DEL: {
            if (wal_) {
                WalRecord rec;
                rec.timestamp_ms = ts;
                rec.op_type      = OpType::DEL;
                rec.key           = cmd.key;
                wal_->append(rec);
            }
            Version v{ts, node_id_};
            engine_.del(cmd.key, v);
            maybe_snapshot();
            return format_ok();
        }

        // ── Phase 5: Replication commands ───────────────────────────────────
        // RSET/RDEL carry an explicit version (timestamp_ms + node_id) chosen
        // by the quorum coordinator so all replicas store identical metadata.

        case CommandType::RSET: {
            Version v{cmd.timestamp_ms, cmd.node_id};
            if (wal_) {
                WalRecord rec;
                rec.timestamp_ms = cmd.timestamp_ms;
                rec.op_type      = OpType::SET;
                rec.key           = cmd.key;
                rec.value         = cmd.value;
                wal_->append(rec);
            }
            engine_.set(cmd.key, cmd.value, v);
            maybe_snapshot();
            return format_ok();
        }

        case CommandType::RDEL: {
            Version v{cmd.timestamp_ms, cmd.node_id};
            if (wal_) {
                WalRecord rec;
                rec.timestamp_ms = cmd.timestamp_ms;
                rec.op_type      = OpType::DEL;
                rec.key           = cmd.key;
                wal_->append(rec);
            }
            engine_.del(cmd.key, v);
            maybe_snapshot();
            return format_ok();
        }

        case CommandType::RGET: {
            // Return value + version so the quorum coordinator can compare
            // across replicas and pick the highest-version response.
            auto result = engine_.get(cmd.key);
            if (!result.found) return format_not_found();
            return format_versioned_value(result.value,
                                          result.version.timestamp_ms,
                                          result.version.node_id);
        }

        default:
            return format_error("INTERNAL");
    }
}

// ── Phase 5: Quorum write ────────────────────────────────────────────────────

std::string Coordinator::quorum_write(const std::string& key,
                                       const std::string& value,
                                       bool is_del) {
    auto replicas = ring_.get_replica_nodes(key, replication_factor_);
    if (replicas.empty()) return format_error("EMPTY_RING");

    // One version shared across all replicas (LWW: coordinator's timestamp
    // + node_id as tiebreaker, per §5.A of CONTEXT.md).
    const Version version{now_ms(), node_id_};

    // Scatter writes to all N replicas in parallel.
    std::atomic<int> acks{0};
    std::vector<std::thread> threads;
    threads.reserve(replicas.size());

    for (const auto& replica : replicas) {
        threads.emplace_back([&, replica]() {
            bool ok = false;

            if (replica.node_id == node_id_) {
                // Local apply: build an RSET/RDEL command with the
                // pre-generated version and call execute_local.
                Command rcmd{};
                rcmd.type         = is_del ? CommandType::RDEL : CommandType::RSET;
                rcmd.key          = key;
                rcmd.value        = value;
                rcmd.timestamp_ms = version.timestamp_ms;
                rcmd.node_id      = version.node_id;
                ok = (execute_local(rcmd) == format_ok());
            } else {
                ok = send_replication_write(replica.address, key, value,
                                            is_del, version);
                // §9.D: if the replica is down, store a hint so we can
                // replay once it comes back UP (Phase 6 will call
                // replay_hints_for() via the heartbeat handler).
                if (!ok) {
                    hints_.store(Hint{
                        replica.address, replica.node_id,
                        key, value, is_del, version
                    });
                }
            }

            if (ok) acks.fetch_add(1, std::memory_order_relaxed);
        });
    }

    for (auto& t : threads) t.join();

    if (acks.load() >= static_cast<int>(write_quorum_)) {
        return format_ok();
    }
    return format_error("QUORUM_FAILED");
}

bool Coordinator::send_replication_write(const std::string& address,
                                          const std::string& key,
                                          const std::string& value,
                                          bool is_del,
                                          const Version& version) {
    auto conn = pool_.acquire(address);
    if (!conn.has_value()) return false;

    // Build RSET or RDEL wire frame.
    std::string frame;
    if (is_del) {
        frame = "RDEL " + std::to_string(key.size()) + " " + key + " "
              + std::to_string(version.timestamp_ms) + " "
              + std::to_string(version.node_id) + "\n";
    } else {
        frame = "RSET " + std::to_string(key.size()) + " " + key + " "
              + std::to_string(value.size()) + " " + value + " "
              + std::to_string(version.timestamp_ms) + " "
              + std::to_string(version.node_id) + "\n";
    }

    ssize_t sent = ::send(conn->fd, frame.data(), frame.size(), MSG_NOSIGNAL);
    if (sent <= 0) {
        ::close(conn->fd);
        return false;
    }

    // Read response (bounded by SO_RCVTIMEO set in ConnectionPool).
    char buf[256];
    std::string response;
    while (true) {
        ssize_t n = ::recv(conn->fd, buf, sizeof(buf), 0);
        if (n > 0) {
            response.append(buf, static_cast<size_t>(n));
            if (response.back() == '\n') break;
        } else {
            ::close(conn->fd);
            return false;
        }
    }

    pool_.release(std::move(*conn));
    return response == "+OK\n";
}

// ── Phase 5: Quorum read (implemented in Increment 3) ───────────────────────

std::string Coordinator::quorum_read(const std::string& key) {
    auto replicas = ring_.get_replica_nodes(key, read_quorum_);
    if (replicas.empty()) return format_error("EMPTY_RING");

    struct ReadResponse {
        bool        ok    = false;
        bool        found = false;
        std::string value;
        Version     version;
        NodeInfo    replica;
    };

    std::vector<ReadResponse> responses(replicas.size());
    std::vector<std::thread>  threads;
    threads.reserve(replicas.size());

    for (size_t i = 0; i < replicas.size(); ++i) {
        threads.emplace_back([&, i]() {
            const auto& rep = replicas[i];
            auto& resp      = responses[i];
            resp.replica    = rep;

            if (rep.node_id == node_id_) {
                resp.ok = true;
                auto r  = engine_.get(key);
                resp.found   = r.found;
                resp.value   = r.value;
                resp.version = r.version;
            } else {
                auto r       = send_replication_read(rep.address, key);
                resp.ok      = r.ok;
                resp.found   = r.found;
                resp.value   = r.value;
                resp.version = r.version;
            }
        });
    }

    for (auto& t : threads) t.join();

    // Pick the highest-version response (§9.C LWW comparison).
    const ReadResponse* best = nullptr;
    int ok_count = 0;
    for (const auto& r : responses) {
        if (!r.ok) continue;
        ++ok_count;
        if (r.found) {
            if (!best || !best->found || is_newer(r.version, best->version)) {
                best = &r;
            }
        }
    }

    if (ok_count == 0) return format_error("QUORUM_FAILED");

    if (!best || !best->found) return format_not_found();

    // Collect stale replicas for async read repair (§9.C).
    std::vector<NodeInfo> stale;
    for (const auto& r : responses) {
        if (!r.ok) continue;
        if (!r.found || is_newer(best->version, r.version)) {
            stale.push_back(r.replica);
        }
    }
    if (!stale.empty()) {
        read_repair_async(key, best->value, best->version, std::move(stale));
    }

    return format_value(best->value);
}

Coordinator::RemoteGetResult Coordinator::send_replication_read(
        const std::string& address, const std::string& key) {
    RemoteGetResult result;

    auto conn = pool_.acquire(address);
    if (!conn.has_value()) return result;

    std::string frame = "RGET " + std::to_string(key.size()) + " " + key + "\n";
    ssize_t sent = ::send(conn->fd, frame.data(), frame.size(), MSG_NOSIGNAL);
    if (sent <= 0) {
        ::close(conn->fd);
        return result;
    }

    char buf[4096];
    std::string response;
    while (true) {
        ssize_t n = ::recv(conn->fd, buf, sizeof(buf), 0);
        if (n > 0) {
            response.append(buf, static_cast<size_t>(n));
            if (response.back() == '\n') break;
        } else {
            ::close(conn->fd);
            return result;
        }
    }

    pool_.release(std::move(*conn));
    result.ok = true;

    auto parsed      = parse_versioned_response(response);
    result.found     = parsed.found;
    result.value     = parsed.value;
    result.version   = Version{parsed.timestamp_ms, parsed.node_id};
    return result;
}

void Coordinator::read_repair_async(const std::string& key,
                                     const std::string& value,
                                     const Version& latest_ver,
                                     std::vector<NodeInfo> stale_replicas) {
    // Fire-and-forget: launch a detached thread so the client response is not
    // delayed.  The repair is best-effort (Phase 5 §9.C of CONTEXT.md).
    std::thread([this, key, value, latest_ver,
                 stale = std::move(stale_replicas)]() {
        for (const auto& replica : stale) {
            if (replica.node_id == node_id_) {
                engine_.set(key, value, latest_ver);
            } else {
                send_replication_write(replica.address, key, value,
                                       false, latest_ver);
            }
        }
    }).detach();
}

// ── Phase 4: Legacy FWD forwarding ──────────────────────────────────────────

std::string Coordinator::forward_to(const std::string& address,
                                     const std::string& inner_line,
                                     uint32_t hops) {
    auto conn = pool_.acquire(address);
    if (!conn.has_value()) {
        return format_error("NODE_UNAVAILABLE");
    }

    // Send the FWD frame
    std::string frame = format_forward(hops, inner_line);
    ssize_t sent = ::send(conn->fd, frame.data(), frame.size(), MSG_NOSIGNAL);
    if (sent <= 0) {
        ::close(conn->fd);  // Don't return bad connection to pool
        return format_error("NODE_UNAVAILABLE");
    }

    // Read the response (blocking, with socket timeout from ConnectionPool)
    char buf[4096];
    std::string response;
    while (true) {
        ssize_t n = ::recv(conn->fd, buf, sizeof(buf), 0);
        if (n > 0) {
            response.append(buf, static_cast<size_t>(n));
            // Check if we have a complete response (ends with \n)
            if (!response.empty() && response.back() == '\n') {
                break;
            }
        } else {
            // Timeout or error
            if (response.empty()) {
                ::close(conn->fd);
                return format_error("NODE_TIMEOUT");
            }
            break;
        }
    }

    // Return connection to pool for reuse
    pool_.release(std::move(*conn));

    return response;
}

std::string Coordinator::serialize_command_line(const Command& cmd) {
    switch (cmd.type) {
        case CommandType::SET:
            return "SET " + std::to_string(cmd.key.size()) + " " + cmd.key +
                   " " + std::to_string(cmd.value.size()) + " " + cmd.value;

        case CommandType::GET:
            return "GET " + std::to_string(cmd.key.size()) + " " + cmd.key;

        case CommandType::DEL:
            return "DEL " + std::to_string(cmd.key.size()) + " " + cmd.key;

        case CommandType::PING:
            return "PING";

        default:
            return "";
    }
}

void Coordinator::maybe_snapshot() {
    if (!wal_ || snapshot_dir_.empty()) return;

    uint64_t ops = ++ops_since_snapshot_;
    if (ops >= snapshot_interval_) {
        ops_since_snapshot_ = 0;
        uint64_t seq = wal_->current_seq_no();
        wal_->sync();
        if (Snapshot::save(engine_, seq, snapshot_dir_)) {
            std::cout << "[SNAP] Snapshot saved at seq " << seq << "\n";
        }
    }
}

// ── Phase 5: Hinted handoff replay ──────────────────────────────────────────

void Coordinator::replay_hints_for(uint32_t target_node_id,
                                    const std::string& target_address) {
    auto pending = hints_.get_hints_for(target_node_id);
    if (pending.empty()) return;

    std::cout << "[HINT] Replaying " << pending.size()
              << " hints for node " << target_node_id
              << " at " << target_address << "\n";

    bool all_ok = true;
    for (const auto& hint : pending) {
        // Use the stored address but allow override with the current address
        // (the node might have a new IP after a restart).
        const std::string& addr = target_address.empty()
                                      ? hint.target_address
                                      : target_address;
        bool ok = send_replication_write(addr, hint.key, hint.value,
                                         hint.is_del, hint.version);
        if (!ok) {
            std::cerr << "[HINT] Replay failed for key '" << hint.key
                      << "' to " << addr << "\n";
            all_ok = false;
        }
    }

    if (all_ok) {
        hints_.clear_hints_for(target_node_id);
        std::cout << "[HINT] All hints replayed and cleared for node "
                  << target_node_id << "\n";
    }
    // If some replays failed, hints are kept for the next retry.
}

}  // namespace dkv
