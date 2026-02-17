#include "config/config.h"
#include "utils/murmurhash3.h"

#include <iostream>

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

    // TODO: start TCP server event loop

    return 0;
}
