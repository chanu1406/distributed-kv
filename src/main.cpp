#include "config/config.h"
#include "network/tcp_server.h"
#include "storage/storage_engine.h"
#include "utils/murmurhash3.h"

#include <csignal>
#include <iostream>

static dkv::TCPServer* g_server = nullptr;

void signal_handler(int) {
    if (g_server) g_server->stop();
}

int main(int argc, char* argv[]) {
    dkv::Config cfg = dkv::parse_args(argc, argv);
    dkv::print_config(cfg);

    // validate quorum invariant: W + R > N
    if (cfg.write_quorum + cfg.read_quorum <= cfg.replication_factor) {
        std::cerr << "[ERROR] Quorum invariant violated: W(" << cfg.write_quorum
                  << ") + R(" << cfg.read_quorum
                  << ") must be > N(" << cfg.replication_factor << ")\n";
        return 1;
    }

    std::cout << "[BOOT] Node " << cfg.node_id
              << " listening on port " << cfg.port << "\n"
              << "[BOOT] Quorum: W=" << cfg.write_quorum
              << " R=" << cfg.read_quorum
              << " N=" << cfg.replication_factor << "\n";

    // Initialize storage engine and TCP server
    dkv::StorageEngine engine;
    dkv::TCPServer server(engine, cfg.port, 4);
    g_server = &server;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    server.run();
    return 0;
}
