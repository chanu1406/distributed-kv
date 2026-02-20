#include "cluster/coordinator.h"

#include <chrono>
#include <cstring>
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

namespace dkv {

Coordinator::Coordinator(StorageEngine& engine, HashRing& ring,
                         ConnectionPool& pool, uint32_t node_id,
                         WAL* wal, const std::string& snapshot_dir,
                         uint64_t snapshot_interval)
    : engine_(engine), ring_(ring), pool_(pool), node_id_(node_id),
      wal_(wal), snapshot_dir_(snapshot_dir),
      snapshot_interval_(snapshot_interval) {}

std::string Coordinator::handle_command(const Command& cmd) {
    // PING is always handled locally
    if (cmd.type == CommandType::PING) {
        return format_pong();
    }

    // FWD: decrement hop counter, then re-parse and handle the inner command
    if (cmd.type == CommandType::FWD) {
        if (cmd.hops_remaining == 0) {
            return format_error("ROUTING_LOOP");
        }

        // Re-parse the inner command (add newline for try_parse)
        std::string inner_with_nl = cmd.inner_line + "\n";
        ParseResult inner_result = try_parse(inner_with_nl.data(),
                                              inner_with_nl.size());
        if (inner_result.status != ParseStatus::OK) {
            return format_error("MALFORMED_FWD");
        }

        // The inner command is expected to be executed locally
        // (we are the target node for this forwarded request)
        return execute_local(inner_result.command);
    }

    // For SET/GET/DEL: check the hash ring to find the owning node
    auto owner = ring_.get_node(cmd.key);
    if (!owner.has_value()) {
        return format_error("EMPTY_RING");
    }

    // If this node owns the key, execute locally
    if (owner->node_id == node_id_) {
        return execute_local(cmd);
    }

    // Forward to the owning node
    std::string inner_line = serialize_command_line(cmd);
    return forward_to(owner->address, inner_line, DEFAULT_HOPS);
}

std::string Coordinator::execute_local(const Command& cmd) {
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
            // Write to WAL first (durability), then apply in memory
            if (wal_) {
                WalRecord rec;
                rec.timestamp_ms = now;
                rec.op_type      = OpType::SET;
                rec.key           = cmd.key;
                rec.value         = cmd.value;
                wal_->append(rec);
            }

            Version v{now, node_id_};
            engine_.set(cmd.key, cmd.value, v);
            maybe_snapshot();
            return format_ok();
        }

        case CommandType::DEL: {
            if (wal_) {
                WalRecord rec;
                rec.timestamp_ms = now;
                rec.op_type      = OpType::DEL;
                rec.key           = cmd.key;
                wal_->append(rec);
            }

            Version v{now, node_id_};
            engine_.del(cmd.key, v);
            maybe_snapshot();
            return format_ok();
        }

        default:
            return format_error("INTERNAL");
    }
}

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

}  // namespace dkv
