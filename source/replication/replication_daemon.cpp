#include <replication/replication_daemon.hpp>
#include <nbio/core/epoll_multiplexer.hpp>
#include <nbio/core/uring_multiplexer.hpp>
#include <nbio/time.hpp>
#include "probe_capture.hpp"
#include <signal.h>
#include <sys/prctl.h>
#include <unistd.h>
#include <cerrno>
#include <charconv>
#include <climits>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string_view>

#include "replay_backlog.hpp"

namespace replication {
namespace {
volatile sig_atomic_t running = 1;
void Stop(int) { running = 0; }

template <typename T>
bool ParseUnsigned(std::string_view text, T& value) {
    if (text.empty()) return false;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

class SignalHandlers {
public:
    SignalHandlers() {
        struct sigaction action{};
        action.sa_handler = Stop;
        sigemptyset(&action.sa_mask);
        if (::sigaction(SIGINT, &action, &old_int_) != 0) {
            throw std::system_error(errno, std::generic_category(), "installing SIGINT handler");
        }
        if (::sigaction(SIGTERM, &action, &old_term_) != 0) {
            const auto error = errno;
            ::sigaction(SIGINT, &old_int_, nullptr);
            throw std::system_error(error, std::generic_category(), "installing SIGTERM handler");
        }
    }
    ~SignalHandlers() {
        ::sigaction(SIGINT, &old_int_, nullptr);
        ::sigaction(SIGTERM, &old_term_, nullptr);
    }
private:
    struct sigaction old_int_{};
    struct sigaction old_term_{};
};

nbio::async::Task<void> Consume(
    ReplayBacklog& backlog, ReplicationDaemon& daemon, ProbeCapture& capture) {
    while (running) {
        capture.Poll(backlog);
        daemon.Maintain();
        co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::milliseconds(10));
    }
}

nbio::async::Task<void> Serve(
    ReplicationDaemon& daemon, ReplayBacklog& backlog, ProbeCapture& capture,
    nbio::net::Address address) {
    struct StopOnExit {
        ReplicationDaemon& daemon;
        ~StopOnExit() { daemon.Stop(); }
    } stop{daemon};
    co_await nbio::async::WhenAny(daemon.Serve(address), Consume(backlog, daemon, capture));
}

void Run(const DaemonOptions& options) {
    if (options.pid <= 0 || options.info_fd < 0 || options.binary.empty() || !options.port || !options.backlog_bytes) {
        throw std::invalid_argument("PID, shared-info fd, binary path, port and backlog size are required");
    }
    const auto address = nbio::net::Address::FromIPv4(options.listen_address, options.port);
    if (!address) throw std::invalid_argument("listen address must be numeric IPv4");
    if (options.multiplexer != "epoll" && options.multiplexer != "io_uring") {
        throw std::invalid_argument("multiplexer must be epoll or io_uring");
    }
    running = 1;
    SignalHandlers signals;
    if (::prctl(PR_SET_PDEATHSIG, SIGTERM) != 0) {
        throw std::system_error(errno, std::generic_category(), "setting repld parent-death signal");
    }
    if (::getppid() != options.pid) throw std::runtime_error("repld must be launched by its master");
    auto info = SharedReplicationInfo::Map(options.info_fd);
    ::close(options.info_fd);
    ProbeCapture capture(options.pid, options.binary,
                         options.bpf_object.empty() ? KVSTORE_BPF_OBJECT : options.bpf_object);
    ReplayBacklog backlog(options.backlog_bytes);
    capture.Poll(backlog);
    if (options.multiplexer == "epoll") {
        nbio::runtime::Daemon::Initialize(std::make_unique<nbio::core::EpollMultiplexer>());
    } else {
        // The follower cap is 128; avoid the backend's much larger default rings.
        nbio::runtime::Daemon::Initialize(std::make_unique<nbio::core::URingMultiplexer>(512, 1024));
    }
    ReplicationDaemon daemon(backlog, info.get());
    std::cerr << "Replication listener " << options.listen_address << ':' << options.port
              << " using " << options.multiplexer << "; write-log uprobe attached\n";
    nbio::async::Run(Serve(daemon, backlog, capture, *address));
}
}  // namespace

int RunDaemon(const DaemonOptions& options) {
    try {
        Run(options);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "repld: " << error.what() << '\n';
        return 1;
    }
}

int DaemonMain(int argc, char** argv) {
    DaemonOptions options;
    const auto usage = [] {
        std::cout << "Usage: repld --pid PID --binary PATH --info-fd FD [--bpf-object PATH] [--listen-address IPv4] "
                     "[--port PORT] [--backlog-bytes BYTES] [--multiplexer epoll|io_uring]\n";
    };
    for (int i = 1; i < argc; ++i) {
        const std::string_view option(argv[i]);
        if (option == "--help" || option == "-h") { usage(); return 0; }
        if (i + 1 == argc) {
            std::cerr << "Missing value for " << option << '\n';
            return 1;
        }
        const std::string_view value(argv[++i]);
        bool valid = true;
        if (option == "--pid") {
            unsigned pid = 0;
            valid = ParseUnsigned(value, pid) && pid > 0 && pid <= static_cast<unsigned>(INT_MAX);
            options.pid = static_cast<int>(pid);
        } else if (option == "--info-fd") {
            unsigned fd = 0;
            valid = ParseUnsigned(value, fd) && fd <= static_cast<unsigned>(INT_MAX);
            options.info_fd = static_cast<int>(fd);
        } else if (option == "--binary") {
            options.binary = value;
        } else if (option == "--bpf-object") {
            options.bpf_object = value;
        } else if (option == "--listen-address") {
            options.listen_address = value;
        } else if (option == "--port") {
            valid = ParseUnsigned(value, options.port) && options.port != 0;
        } else if (option == "--backlog-bytes") {
            valid = ParseUnsigned(value, options.backlog_bytes) && options.backlog_bytes != 0;
        } else if (option == "--multiplexer") {
            valid = value == "epoll" || value == "io_uring";
            options.multiplexer = value;
        } else {
            valid = false;
        }
        if (!valid) {
            std::cerr << "Invalid option or value: " << option << '\n';
            return 1;
        }
    }
    if (options.pid <= 0 || options.info_fd < 0 || options.binary.empty()) { usage(); return 1; }
    return RunDaemon(options);
}
}  // namespace replication
