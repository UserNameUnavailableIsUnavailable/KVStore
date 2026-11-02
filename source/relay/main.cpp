// The replication relay daemon. It mirrors the master's realtime push stream to
// every slave that connects, without the master knowing it exists.
//
// Two upstreams, one contract:
//   - eBPF: relay.bpf.o attaches to TC egress on `lo` and copies the payload of
//     the master's pushes to its slaves into a ring buffer; the daemon forwards
//     those bytes to connected slaves. No master connection, fully passive.
//   - fallback: if the object cannot be loaded or attached (no privileges), the
//     daemon registers with the master as a /dev/null replica and forwards the
//     bytes it is pushed, exactly like the broker.
//
// The daemon never interprets the stream, so a slave finds its own starting
// offset in the first frame it receives -- the same contract the master and the
// broker offer. A slow slave is dropped; its offset check makes it resync.

#include <relay/relay.hpp>

#include <arpa/inet.h>
#include <fcntl.h>
#include <net/if.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bpf/libbpf.h>
#include <relay/relay_event.h>

#include <cerrno>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

#ifndef RELAY_BPF_OBJECT
#define RELAY_BPF_OBJECT ""
#endif

namespace {
volatile sig_atomic_t g_stop = 0;
void OnSignal(int) { g_stop = 1; }

struct Subscriber {
    int fd = -1;
    std::string out;
};
std::vector<Subscriber> g_subscribers;

bool ParseUnsigned(std::string_view text, std::uint64_t& value) {
    if (text.empty()) return false;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

bool SplitEndpoint(std::string_view text, std::string& ip, std::uint16_t& port) {
    const auto colon = text.rfind(':');
    std::uint64_t value = 0;
    if (colon == std::string_view::npos || !ParseUnsigned(text.substr(colon + 1), value) || value == 0 ||
        value > 65535) {
        return false;
    }
    ip.assign(text.substr(0, colon));
    port = static_cast<std::uint16_t>(value);
    return !ip.empty();
}

std::string RespCommand(std::initializer_list<std::string_view> parts) {
    std::string out = "*" + std::to_string(parts.size()) + "\r\n";
    for (const auto part : parts) {
        out += "$" + std::to_string(part.size()) + "\r\n";
        out.append(part.data(), part.size());
        out += "\r\n";
    }
    return out;
}

int ConnectTo(const std::string& ip, std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    if (::inet_pton(AF_INET, ip.c_str(), &address.sin_addr) != 1) {
        ::close(fd);
        return -1;
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        ::close(fd);
        return -1;
    }
    const int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
    return fd;
}

void Broadcast(const void* data, std::size_t length) {
    if (length == 0) return;
    for (auto& sub : g_subscribers) {
        if (sub.fd < 0) continue;
        if (sub.out.size() + length > (16U << 20)) {
            ::close(sub.fd);
            sub.fd = -1;
            sub.out.clear();
            continue;
        }
        sub.out.append(static_cast<const char*>(data), length);
    }
}

bool Flush(Subscriber& sub) {
    while (!sub.out.empty()) {
        const auto sent = ::send(sub.fd, sub.out.data(), sub.out.size(), MSG_NOSIGNAL);
        if (sent < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
            ::close(sub.fd);
            sub.fd = -1;
            sub.out.clear();
            return false;
        }
        sub.out.erase(0, static_cast<std::size_t>(sent));
    }
    return true;
}

// Reads exactly one line (up to CRLF) a byte at a time, so no frame byte past the
// master's greeting is consumed by accident.
std::string ReadGreeting(int fd) {
    std::string line;
    char byte = 0;
    while (line.size() < 512) {
        const auto got = ::recv(fd, &byte, 1, 0);
        if (got == 1) {
            line.push_back(byte);
            if (line.size() >= 2 && line[line.size() - 2] == '\r' && line.back() == '\n') break;
            continue;
        }
        if (got < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            ::pollfd wait{fd, POLLIN, 0};
            ::poll(&wait, 1, 500);
            continue;
        }
        break;
    }
    return line;
}

int OnEvent(void*, void* data, std::size_t) {
    const auto* event = static_cast<const relay_packet_event*>(data);
    if (event->chunk_len > 0) {
        Broadcast(event->data, event->chunk_len);
    }
    return 0;
}
}  // namespace

int main(int argc, char** argv) {
    std::string listen_endpoint;
    std::string master_endpoint;
    std::string ifname = "lo";
    std::uint64_t master_port = 6666;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        if (arg == "--listen" && i + 1 < argc) {
            listen_endpoint = argv[++i];
        } else if (arg == "--master" && i + 1 < argc) {
            master_endpoint = argv[++i];
        } else if ((arg == "--ifname" || arg == "--ifindex") && i + 1 < argc) {
            ifname = argv[++i];
        } else if (arg == "--master-port" && i + 1 < argc) {
            ParseUnsigned(argv[++i], master_port);
        } else if (arg == "-h" || arg == "--help") {
            std::printf("usage: relay --listen <ip:port> [--master <ip:port>] [--ifname lo] [--master-port 6666]\n");
            return 0;
        }
    }
    std::string listen_ip;
    std::uint16_t listen_port = 0;
    if (!SplitEndpoint(listen_endpoint, listen_ip, listen_port)) {
        std::fprintf(stderr, "usage: relay --listen <ip:port> [--master <ip:port>] [--ifname lo]\n");
        return 2;
    }
    std::string master_ip;
    std::uint16_t master_port16 = 0;
    if (!master_endpoint.empty() && !SplitEndpoint(master_endpoint, master_ip, master_port16)) {
        std::fprintf(stderr, "relay: invalid --master endpoint\n");
        return 2;
    }

    struct sigaction action {};
    action.sa_handler = OnSignal;
    ::sigaction(SIGINT, &action, nullptr);
    ::sigaction(SIGTERM, &action, nullptr);

    const int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        std::perror("socket");
        return 1;
    }
    const int one = 1;
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    ::fcntl(listen_fd, F_SETFL, ::fcntl(listen_fd, F_GETFL, 0) | O_NONBLOCK);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(listen_port);
    if (::inet_pton(AF_INET, listen_ip.c_str(), &address.sin_addr) != 1 ||
        ::bind(listen_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 || ::listen(listen_fd, 64) < 0) {
        std::perror("bind/listen");
        return 1;
    }

    // Prefer the passive eBPF tap; fall back to being a /dev/null replica.
    relay::RelayCapture capture;
    bool captured = capture.Open(RELAY_BPF_OBJECT, ::if_nametoindex(ifname.c_str()), static_cast<std::uint16_t>(master_port));
    std::fprintf(stderr, "relay: listen %s:%u, upstream %s\n", listen_ip.c_str(), listen_port,
                 captured ? "eBPF tap (TC egress)" : (master_endpoint.empty() ? "none" : "TCP replica"));

    ring_buffer* rb = nullptr;
    if (captured) {
        rb = ring_buffer__new(capture.RingFd(), OnEvent, nullptr, nullptr);
        if (rb == nullptr) {
            std::fprintf(stderr, "relay: cannot create the ring buffer; falling back to a TCP replica\n");
            captured = false;
        }
    }

    int upstream = -1;
    bool need_register = false;
    bool greeted = false;
    std::string up_out;

    while (!g_stop) {
        if (captured) {
            const int error = ring_buffer__poll(rb, 100);
            if (error < 0 && error != -EINTR) {
                std::fprintf(stderr, "relay: ring buffer poll failed (%d)\n", error);
                break;
            }
        } else {
            // Maintain the fallback upstream connection.
            if (upstream < 0) {
                if (master_endpoint.empty()) {
                    std::fprintf(stderr, "relay: no BPF tap and no --master endpoint; nothing to forward\n");
                    ::usleep(500000);
                    continue;
                }
                upstream = ConnectTo(master_ip, master_port16);
                if (upstream < 0) {
                    ::usleep(200000);
                    continue;
                }
                need_register = true;
                greeted = false;
            }
            if (need_register) {
                up_out += RespCommand({"REPLICA"});
                need_register = false;
            }
            while (!up_out.empty()) {
                const auto sent = ::send(upstream, up_out.data(), up_out.size(), MSG_NOSIGNAL);
                if (sent > 0) {
                    up_out.erase(0, static_cast<std::size_t>(sent));
                } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                    break;
                } else {
                    ::close(upstream);
                    upstream = -1;
                    break;
                }
            }
            if (upstream < 0) continue;

            if (!greeted) {
                std::string greeting = ReadGreeting(upstream);
                if (greeting.empty()) {
                    ::close(upstream);
                    upstream = -1;
                    continue;
                }
                greeted = true;
            } else {
                char chunk[65536];
                while (true) {
                    const auto got = ::recv(upstream, chunk, sizeof(chunk), 0);
                    if (got > 0) {
                        Broadcast(chunk, static_cast<std::size_t>(got));
                        continue;
                    }
                    if (got == 0) {
                        ::close(upstream);
                        upstream = -1;
                    } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                        ::close(upstream);
                        upstream = -1;
                    }
                    break;
                }
            }
        }

        // Accept slaves and flush their queues.
        std::vector<pollfd> fds;
        fds.push_back({listen_fd, POLLIN, 0});
        std::vector<std::size_t> slots;
        for (std::size_t i = 0; i < g_subscribers.size(); ++i) {
            if (g_subscribers[i].fd < 0) continue;
            fds.push_back({g_subscribers[i].fd, static_cast<short>(g_subscribers[i].out.empty() ? POLLIN : POLLOUT), 0});
            slots.push_back(i);
        }
        const int ready = ::poll(fds.data(), fds.size(), captured ? 100 : 50);
        if (ready < 0 && errno != EINTR) break;

        if (fds[0].revents & POLLIN) {
            while (true) {
                const int fd = ::accept(listen_fd, nullptr, nullptr);
                if (fd < 0) break;
                ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
                ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                g_subscribers.push_back(Subscriber{fd, {}});
                std::fprintf(stderr, "relay: slave attached (fd %d)\n", fd);
            }
        }
        for (std::size_t i = 0; i < slots.size(); ++i) {
            const std::size_t index = 1 + i;
            if (index >= fds.size()) break;
            auto& sub = g_subscribers[slots[i]];
            if (sub.fd < 0) continue;
            if (fds[index].revents & (POLLHUP | POLLERR)) {
                ::close(sub.fd);
                sub.fd = -1;
                continue;
            }
            if (fds[index].revents & POLLOUT) {
                Flush(sub);
            }
        }
        std::erase_if(g_subscribers, [](const Subscriber& sub) { return sub.fd < 0; });
    }

    if (rb != nullptr) {
        ring_buffer__free(rb);
    }
    for (auto& sub : g_subscribers) {
        if (sub.fd >= 0) ::close(sub.fd);
    }
    ::close(listen_fd);
    return 0;
}
