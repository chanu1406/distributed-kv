#include "cluster/cluster_config.h"

#include <fstream>
#include <iostream>
#include <sstream>

namespace dkv {

std::vector<NodeEntry> parse_cluster_config(const std::string& filepath) {
    std::vector<NodeEntry> entries;

    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "[CONFIG] Cannot open cluster config: " << filepath << "\n";
        return entries;
    }

    std::string line;
    int line_num = 0;

    while (std::getline(file, line)) {
        line_num++;

        // Trim leading whitespace
        size_t start = line.find_first_not_of(" \t\r");
        if (start == std::string::npos) continue;  // blank line
        line = line.substr(start);

        // Skip comments
        if (line[0] == '#') continue;

        // Parse: <name> <host>:<port>
        std::istringstream iss(line);
        std::string name, address;
        if (!(iss >> name >> address)) {
            std::cerr << "[CONFIG] Skipping malformed line " << line_num
                      << ": " << line << "\n";
            continue;
        }

        // Split address into host:port
        auto colon_pos = address.rfind(':');
        if (colon_pos == std::string::npos || colon_pos == 0 ||
            colon_pos == address.size() - 1) {
            std::cerr << "[CONFIG] Skipping malformed address on line "
                      << line_num << ": " << address << "\n";
            continue;
        }

        std::string host = address.substr(0, colon_pos);
        std::string port_str = address.substr(colon_pos + 1);

        // Parse port
        uint16_t port = 0;
        try {
            int p = std::stoi(port_str);
            if (p <= 0 || p > 65535) {
                std::cerr << "[CONFIG] Invalid port on line " << line_num
                          << ": " << port_str << "\n";
                continue;
            }
            port = static_cast<uint16_t>(p);
        } catch (...) {
            std::cerr << "[CONFIG] Invalid port on line " << line_num
                      << ": " << port_str << "\n";
            continue;
        }

        entries.push_back({name, host, port});
    }

    return entries;
}

}  // namespace dkv
