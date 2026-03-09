// dkv_cli.cpp — Interactive REPL client for the distributed KV store.
// Equivalent of redis-cli: connects to a running dkv_node over TCP.
// Pure POSIX TCP client — zero dkv_core dependency.
//
// Usage: ./bin/dkv_cli [--host H] [-h H] [--port P] [-p P] [--help]
//
// Compile target: dkv_cli (see CMakeLists.txt)
// Standard: C++20, POSIX only (Linux / macOS)

#include <cctype>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

// POSIX socket headers (Linux / macOS only)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

// ── Signal flag ───────────────────────────────────────────────────────────────

static volatile sig_atomic_t g_quit = 0;

static void handle_sigint(int /*sig*/) {
    g_quit = 1;
}

// ── TCP helpers ───────────────────────────────────────────────────────────────

// Open a blocking TCP connection with TCP_NODELAY and 5s timeouts.
// Returns a valid fd on success, -1 on failure.
static int open_connection(const std::string& host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    // Disable Nagle's algorithm for low-latency interactive use.
    int one = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                     &one, static_cast<socklen_t>(sizeof(one)));

    // 5-second send/receive timeout (spec: SO_RCVTIMEO/SO_SNDTIMEO = 5s).
    struct timeval tv{};
    tv.tv_sec  = 5;
    tv.tv_usec = 0;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,
                     &tv, static_cast<socklen_t>(sizeof(tv)));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO,
                     &tv, static_cast<socklen_t>(sizeof(tv)));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }
    if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr),
                static_cast<socklen_t>(sizeof(addr))) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

// Send exactly `len` bytes. Returns false on failure.
static bool send_all(int fd, const char* buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// Read bytes until '\n'. Returns the line including '\n', or "" on error/timeout.
static std::string recv_line(int fd) {
    std::string line;
    line.reserve(128);
    char c = '\0';
    while (true) {
        ssize_t n = recv(fd, &c, 1, 0);
        if (n <= 0) return "";
        line += c;
        if (c == '\n') return line;
    }
}

// ── Protocol formatters ───────────────────────────────────────────────────────

static std::string fmt_set(const std::string& key, const std::string& val) {
    return "SET " + std::to_string(key.size()) + " " + key + " "
         + std::to_string(val.size()) + " " + val + "\n";
}

static std::string fmt_get(const std::string& key) {
    return "GET " + std::to_string(key.size()) + " " + key + "\n";
}

static std::string fmt_del(const std::string& key) {
    return "DEL " + std::to_string(key.size()) + " " + key + "\n";
}

// ── Response parser ───────────────────────────────────────────────────────────
//
// Reads one response line from `fd` and prints it in human-friendly form.
//
// Returns:
//   true  — response handled; REPL should continue.
//   false — connection was lost; caller should exit.
//
// `timed_out` is set to true when SO_RCVTIMEO fires (EAGAIN/EWOULDBLOCK).
// On timeout the error is already printed and the caller should re-prompt.

static bool print_response(int fd, bool& timed_out) {
    timed_out = false;

    std::string line = recv_line(fd);

    if (line.empty()) {
        // recv returned <= 0. Distinguish timeout from real closure.
        int err = errno;
        if (err == EAGAIN || err == EWOULDBLOCK) {
            timed_out = true;
            std::cout << "Error: request timed out\n";
            return true;   // Don't exit — let the user retry.
        }
        if (g_quit || err == EINTR) {
            // SIGINT interrupted the recv; treat as clean exit.
            return true;
        }
        return false;  // Connection closed or other error.
    }

    // Strip trailing '\n' (and '\r' if present).
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r'))
        line.pop_back();

    if (line.empty()) {
        std::cout << "(empty response)\n";
        return true;
    }

    char prefix = line[0];

    if (prefix == '+') {
        // +OK  or  +PONG
        std::cout << line.substr(1) << "\n";

    } else if (prefix == '$') {
        // $<len> <value>  — extract everything after the first space.
        auto sp = line.find(' ');
        if (sp != std::string::npos && sp + 1 < line.size()) {
            std::cout << "\"" << line.substr(sp + 1) << "\"\n";
        } else {
            std::cout << "(empty value)\n";
        }

    } else if (prefix == '-') {
        std::string body = line.substr(1);
        if (body == "NOT_FOUND") {
            std::cout << "(nil)\n";
        } else if (body.rfind("ERR ", 0) == 0) {
            std::cout << "(error) " << body.substr(4) << "\n";
        } else {
            // Catch-all: e.g. -QUORUM_FAILED
            std::cout << "(error) " << body << "\n";
        }

    } else {
        // Unexpected — show as-is for debugging.
        std::cout << line << "\n";
    }

    return true;
}

// ── Input tokenizer ───────────────────────────────────────────────────────────
//
// Splits `line` into tokens on whitespace.
// Double-quoted strings are treated as a single token (quotes are stripped).
//   "SET key \"hello world\""  →  ["SET", "key", "hello world"]
// If there is no closing '"', the '"' is treated as a regular character.

static std::vector<std::string> tokenize(const std::string& line) {
    std::vector<std::string> tokens;
    size_t i = 0;
    const size_t n = line.size();

    while (i < n) {
        // Skip whitespace.
        while (i < n && std::isspace(static_cast<unsigned char>(line[i]))) ++i;
        if (i >= n) break;

        if (line[i] == '"') {
            // Quoted token: collect until closing '"'.
            ++i;  // consume opening '"'
            std::string tok;
            size_t close = line.find('"', i);
            if (close != std::string::npos) {
                tok = line.substr(i, close - i);
                i = close + 1;  // consume closing '"'
            } else {
                // No closing quote — treat '"' as regular char.
                tok = '"' + line.substr(i);
                i = n;
            }
            tokens.push_back(std::move(tok));
        } else {
            // Plain token: collect until whitespace.
            std::string tok;
            while (i < n && !std::isspace(static_cast<unsigned char>(line[i])))
                tok += line[i++];
            tokens.push_back(std::move(tok));
        }
    }

    return tokens;
}

// ── String utilities ──────────────────────────────────────────────────────────

static std::string to_upper(const std::string& s) {
    std::string result;
    result.reserve(s.size());
    for (char c : s)
        result += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return result;
}

static std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    size_t end   = s.find_last_not_of(" \t\r\n");
    return (start == std::string::npos) ? "" : s.substr(start, end - start + 1);
}

// ── Help / usage text ─────────────────────────────────────────────────────────

static void print_help() {
    std::cout <<
        "Commands:\n"
        "  SET <key> <value>   Set a key-value pair\n"
        "  GET <key>           Get a value by key\n"
        "  DEL <key>           Delete a key\n"
        "  PING                Check server connectivity\n"
        "  QUIT / EXIT         Close connection and exit\n"
        "  HELP                Show this message\n";
}

static void print_usage(const char* prog) {
    std::cout <<
        "Usage: " << prog << " [OPTIONS]\n"
        "\n"
        "Options:\n"
        "  -h, --host HOST   Server hostname or IP  (default: 127.0.0.1)\n"
        "  -p, --port PORT   Server port number     (default: 7001)\n"
        "      --help        Show this message and exit\n"
        "\n"
        "Example:\n"
        "  " << prog << " --host 127.0.0.1 --port 7002\n";
}

// ── REPL ──────────────────────────────────────────────────────────────────────

// Returns true if the session ended cleanly (QUIT/EXIT/Ctrl-D/SIGINT).
// Returns false if the connection was lost unexpectedly.
static bool run_repl(int fd) {
    static const char k_ping[] = "PING\n";

    std::string line;
    while (!g_quit) {
        std::cout << "dkv> " << std::flush;

        if (!std::getline(std::cin, line)) {
            if (g_quit) break;
            // Real EOF (Ctrl-D).
            std::cout << "\nBye.\n";
            return true;
        }

        if (g_quit) break;

        line = trim(line);
        if (line.empty()) continue;

        auto tokens = tokenize(line);
        if (tokens.empty()) continue;

        std::string cmd = to_upper(tokens[0]);

        // ── QUIT / EXIT ───────────────────────────────────────────────────────
        if (cmd == "QUIT" || cmd == "EXIT") {
            std::cout << "Bye.\n";
            return true;
        }

        // ── HELP (local, no network) ──────────────────────────────────────────
        if (cmd == "HELP") {
            print_help();
            continue;
        }

        // ── PING ──────────────────────────────────────────────────────────────
        if (cmd == "PING") {
            if (!send_all(fd, k_ping, sizeof(k_ping) - 1)) {
                std::cerr << "Error: connection lost\n";
                return false;
            }
            bool timed_out = false;
            if (!print_response(fd, timed_out) && !timed_out) {
                std::cerr << "Error: connection lost\n";
                return false;
            }
            continue;
        }

        // ── SET ───────────────────────────────────────────────────────────────
        if (cmd == "SET") {
            if (tokens.size() != 3u) {
                std::cout << "(error) Usage: SET <key> <value>\n";
                continue;
            }
            std::string req = fmt_set(tokens[1], tokens[2]);
            if (!send_all(fd, req.c_str(), req.size())) {
                std::cerr << "Error: connection lost\n";
                return false;
            }
            bool timed_out = false;
            if (!print_response(fd, timed_out) && !timed_out) {
                std::cerr << "Error: connection lost\n";
                return false;
            }
            continue;
        }

        // ── GET ───────────────────────────────────────────────────────────────
        if (cmd == "GET") {
            if (tokens.size() != 2u) {
                std::cout << "(error) Usage: GET <key>\n";
                continue;
            }
            std::string req = fmt_get(tokens[1]);
            if (!send_all(fd, req.c_str(), req.size())) {
                std::cerr << "Error: connection lost\n";
                return false;
            }
            bool timed_out = false;
            if (!print_response(fd, timed_out) && !timed_out) {
                std::cerr << "Error: connection lost\n";
                return false;
            }
            continue;
        }

        // ── DEL ───────────────────────────────────────────────────────────────
        if (cmd == "DEL") {
            if (tokens.size() != 2u) {
                std::cout << "(error) Usage: DEL <key>\n";
                continue;
            }
            std::string req = fmt_del(tokens[1]);
            if (!send_all(fd, req.c_str(), req.size())) {
                std::cerr << "Error: connection lost\n";
                return false;
            }
            bool timed_out = false;
            if (!print_response(fd, timed_out) && !timed_out) {
                std::cerr << "Error: connection lost\n";
                return false;
            }
            continue;
        }

        // ── Unknown ───────────────────────────────────────────────────────────
        std::cout << "Unknown command. Type HELP for usage.\n";
    }

    // Reached via g_quit (SIGINT).
    std::cout << "\nBye.\n";
    return true;
}

// ── Main ──────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int         port = 7001;

    // Parse CLI flags.
    for (int i = 1; i < argc; ++i) {
        if ((std::strcmp(argv[i], "--host") == 0 ||
             std::strcmp(argv[i], "-h") == 0) && i + 1 < argc) {
            host = argv[++i];
        } else if ((std::strcmp(argv[i], "--port") == 0 ||
                    std::strcmp(argv[i], "-p") == 0) && i + 1 < argc) {
            port = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            std::cerr << "Unknown option: " << argv[i] << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    // Install SIGINT handler so Ctrl-C cleanly closes the connection.
    struct sigaction sa{};
    sa.sa_handler = handle_sigint;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;  // do NOT set SA_RESTART: we want getline/recv to be interrupted
    sigaction(SIGINT, &sa, nullptr);

    // Connect to the node.
    int fd = open_connection(host, port);
    if (fd < 0) {
        std::cerr << "Error: could not connect to " << host << ":" << port << "\n";
        return 1;
    }

    std::cout << "Connected to " << host << ":" << port << "\n";

    // Run the REPL; it owns the connection for its lifetime.
    bool clean = run_repl(fd);

    close(fd);
    return clean ? 0 : 1;
}
