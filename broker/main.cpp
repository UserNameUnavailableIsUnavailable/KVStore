// The replication broker: registers with a master as a `/dev/null` replica and
// blindly forwards the bytes the master pushes to whichever slave is attached.
//
// Forwarding is done in the kernel with an sk_skb + SOCKHASH redirect
// (broker/bpf/redirect.bpf.c): the upstream socket sits at key 0 and the
// attached slave at key 1, so ingress from the master is copied to the slave
// with no userspace copy and no parsing. When BPF cannot be loaded (no
// privileges) the broker falls back to a plain userspace byte relay, which
// keeps the same "blind" contract.
//
// A slave may attach at any time; it receives everything pushed from that
// moment. The broker never interprets the stream, so a slave must find its own
// starting offset in the first frame it receives.

#include "redirect.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

// The BPF object is compiled by bpf/redirect.bpf.c and its path is injected by
// CMake. Without it the broker relays in userspace, so an empty default is valid.
#ifndef BROKER_BPF_OBJECT
#define BROKER_BPF_OBJECT ""
#endif

namespace {
volatile sig_atomic_t g_stop = 0;
void OnSignal(int) { g_stop = 1; }

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

struct Subscriber {
    int fd = -1;
    std::string out;
};

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
// greeting is consumed before the redirect takes over.
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
        break;  // closed or error
    }
    return line;
}
}  // namespace

int main(int argc, char** argv) {
    std::string master_ip;
    std::uint16_t master_port = 0;
    std::string listen_ip = "0.0.0.0";
    std::uint16_t listen_port = 0;
    std::string master_endpoint;
    std::string listen_endpoint;
    bool want_bpf = true;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        if (arg == "--master" && i + 1 < argc) {
            master_endpoint = argv[++i];
        } else if (arg == "--listen" && i + 1 < argc) {
            listen_endpoint = argv[++i];
        } else if (arg == "--no-bpf") {
            want_bpf = false;
        } else if (arg == "-h" || arg == "--help") {
            std::printf("usage: broker --master <ip:port> --listen <ip:port> [--no-bpf]\n");
            return 0;
        }
    }
    if (!SplitEndpoint(master_endpoint, master_ip, master_port) ||
        !SplitEndpoint(listen_endpoint, listen_ip, listen_port)) {
        std::fprintf(stderr, "usage: broker --master <ip:port> --listen <ip:port> [--no-bpf]\n");
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

    broker::SockmapRedirect redirect;
    bool bpf = want_bpf && redirect.Open(BROKER_BPF_OBJECT);
    std::fprintf(stderr, "broker: master %s:%u listen %s:%u, forwarding %s\n", master_ip.c_str(), master_port,
                 listen_ip.c_str(), listen_port, bpf ? "in-kernel (sk_skb+sockhash)" : "userspace relay");

    int upstream = -1;
    bool need_register = false;
    bool greeted = false;
    std::string up_out;
    int slave_fd = -1;                 // BPF mode: the one attached slave
    std::vector<Subscriber> subscribers;  // relay mode: every attached slave

    const auto drop_slaves = [&] {
        if (slave_fd >= 0) {
            redirect.DetachSlave();
            ::close(slave_fd);
            slave_fd = -1;
        }
        for (auto& sub : subscribers) {
            if (sub.fd >= 0) ::close(sub.fd);
        }
        subscribers.clear();
    };
    const auto reset_upstream = [&] {
        if (upstream >= 0) ::close(upstream);
        upstream = -1;
        up_out.clear();
        greeted = false;
        need_register = false;
        drop_slaves();
    };

    while (!g_stop) {
        if (upstream < 0) {
            upstream = ConnectTo(master_ip, master_port);
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
                reset_upstream();
                break;
            }
        }
        if (upstream < 0) continue;

        if (!greeted) {
            // Consume the master's +CONTINUE line so it is not forwarded as data.
            std::string greeting = ReadGreeting(upstream);
            if (greeting.empty()) {
                reset_upstream();
                continue;
            }
            greeted = true;
            if (bpf && !redirect.AttachUpstream(upstream)) {
                std::fprintf(stderr, "broker: sockmap attach failed; switching to relay\n");
                bpf = false;
            }
        }

        std::vector<pollfd> fds;
        fds.push_back({listen_fd, POLLIN, 0});
        // With the redirect installed the kernel consumes the upstream's data, so
        // there is nothing for us to read; asking for POLLIN there would only spin.
        fds.push_back({upstream, static_cast<short>(bpf ? 0 : POLLIN), 0});
        std::vector<int> sub_slot;
        if (bpf) {
            if (slave_fd >= 0) fds.push_back({slave_fd, POLLIN | POLLHUP | POLLERR, 0});
        } else {
            for (std::size_t i = 0; i < subscribers.size(); ++i) {
                if (subscribers[i].fd < 0) continue;
                fds.push_back({subscribers[i].fd, static_cast<short>(subscribers[i].out.empty() ? POLLIN : POLLOUT), 0});
                sub_slot.push_back(static_cast<int>(i));
            }
        }

        const int ready = ::poll(fds.data(), fds.size(), 500);
        if (ready < 0 && errno != EINTR) break;

        // Accept a slave.
        if (fds[0].revents & POLLIN) {
            while (true) {
                const int fd = ::accept(listen_fd, nullptr, nullptr);
                if (fd < 0) break;
                ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
                ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                if (bpf) {
                    if (slave_fd >= 0) {
                        redirect.DetachSlave();
                        ::close(slave_fd);
                    }
                    slave_fd = fd;
                    if (!redirect.AttachSlave(fd)) {
                        ::close(fd);
                        slave_fd = -1;
                    }
                } else {
                    subscribers.push_back(Subscriber{fd, {}});
                }
                std::fprintf(stderr, "broker: slave attached (fd %d)\n", fd);
            }
        }

        // Upstream events.
        if (fds[1].revents & (POLLHUP | POLLERR)) {
            std::fprintf(stderr, "broker: master connection lost; reconnecting\n");
            reset_upstream();
            continue;
        }
        if (!bpf && (fds[1].revents & POLLIN)) {
            char chunk[65536];
            bool broken = false;
            while (true) {
                const auto got = ::recv(upstream, chunk, sizeof(chunk), 0);
                if (got > 0) {
                    for (auto& sub : subscribers) {
                        if (sub.fd < 0) continue;
                        if (sub.out.size() + static_cast<std::size_t>(got) > (16U * 1024U * 1024U)) {
                            ::close(sub.fd);
                            sub.fd = -1;
                            sub.out.clear();
                            continue;
                        }
                        sub.out.append(chunk, static_cast<std::size_t>(got));
                    }
                    continue;
                }
                if (got == 0) broken = true;
                else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) broken = true;
                break;
            }
            if (broken) {
                reset_upstream();
                continue;
            }
        }

        // Slave events.
        if (bpf) {
            if (slave_fd >= 0 && fds.size() > 2 && (fds[2].revents & (POLLHUP | POLLERR))) {
                std::fprintf(stderr, "broker: slave detached\n");
                redirect.DetachSlave();
                ::close(slave_fd);
                slave_fd = -1;
            }
        } else {
            for (std::size_t i = 0; i < sub_slot.size(); ++i) {
                const std::size_t index = 2 + i;
                if (index >= fds.size()) break;
                auto& sub = subscribers[sub_slot[i]];
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
            // Retire the slaves that hung up or were dropped for being too slow.
            std::erase_if(subscribers, [](const Subscriber& sub) { return sub.fd < 0; });
        }
    }

    reset_upstream();
    ::close(listen_fd);
    return 0;
}
