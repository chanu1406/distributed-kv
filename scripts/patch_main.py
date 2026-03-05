import sys

path = '/mnt/c/Users/ochan/OneDrive/Desktop/test/distributedKV/src/main.cpp'

with open(path, 'r') as f:
    lines = f.readlines()

insert_idx = None
for i, line in enumerate(lines):
    if 'Create TCP server in cluster mode' in line:
        insert_idx = i
        break

if insert_idx is None:
    print('ERROR: marker line not found')
    sys.exit(1)

new_lines = []
new_lines.append('\n')
new_lines.append('    // Phase 6: Build membership tracker\n')
new_lines.append('    dkv::Membership membership(3, static_cast<int>(cfg.heartbeat_timeout_ms));\n')
new_lines.append('\n')
new_lines.append('    for (const auto& entry : cluster_entries) {\n')
new_lines.append('        uint32_t id = 0;\n')
new_lines.append('        for (char c : entry.name) {\n')
new_lines.append('            if (c >= \'0\' && c <= \'9\') id = id * 10 + static_cast<uint32_t>(c - \'0\');\n')
new_lines.append('        }\n')
new_lines.append('        if (id == 0) id = static_cast<uint32_t>(std::hash<std::string>{}(entry.name) & 0xFFFFFFFF);\n')
new_lines.append('        if (id == cfg.node_id) continue;\n')
new_lines.append('        std::string addr = entry.host + ":" + std::to_string(entry.port);\n')
new_lines.append('        membership.add_peer(id, addr);\n')
new_lines.append('    }\n')
new_lines.append('\n')
new_lines.append('    membership.set_rejoin_callback([&coordinator](uint32_t node_id, const std::string& addr) {\n')
new_lines.append('        std::cout << "[MEMBERSHIP] Node " << node_id << " at " << addr\n')
new_lines.append('                  << " is UP - replaying hints\n";\n')
new_lines.append('        coordinator.replay_hints_for(node_id, addr);\n')
new_lines.append('    });\n')
new_lines.append('\n')
new_lines.append('    membership.set_down_callback([](uint32_t node_id, const std::string& addr) {\n')
new_lines.append('        std::cout << "[MEMBERSHIP] Node " << node_id << " at " << addr << " is DOWN\n";\n')
new_lines.append('    });\n')
new_lines.append('\n')
new_lines.append('    // Phase 6: Start heartbeat\n')
new_lines.append('    dkv::Heartbeat heartbeat(membership, cfg.node_id,\n')
new_lines.append('                             static_cast<int>(cfg.heartbeat_interval_ms), 500);\n')
new_lines.append('    g_heartbeat = &heartbeat;\n')
new_lines.append('    heartbeat.start();\n')
new_lines.append('    std::cout << "[BOOT] Heartbeat started (interval=" << cfg.heartbeat_interval_ms\n')
new_lines.append('              << "ms, timeout=" << cfg.heartbeat_timeout_ms << "ms)\n";\n')
new_lines.append('\n')

lines = lines[:insert_idx] + new_lines + lines[insert_idx:]

# Insert heartbeat.stop() after server.run()
for i, line in enumerate(lines):
    if 'server.run();' in line:
        lines.insert(i + 1, '    heartbeat.stop();\n')
        break

with open(path, 'w') as f:
    f.writelines(lines)
print('Done - inserted', len(new_lines), 'lines at index', insert_idx)
