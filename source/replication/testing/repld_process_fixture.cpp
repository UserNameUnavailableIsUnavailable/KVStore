#include <replication/replication_info.hpp>
#include <signal.h>
#include <chrono>
#include <charconv>
#include <string_view>
#include <thread>

int main(int argc, char** argv) {
    int fd = -1;
    bool exit_after_ready = false;
    bool never_ready = false;
    for (int i = 1; i + 1 < argc; i += 2) {
        const std::string_view option(argv[i]), value(argv[i + 1]);
        if (option == "--info-fd") {
            const auto parsed = std::from_chars(value.data(), value.data() + value.size(), fd);
            if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) return 2;
        }
        if (option == "--multiplexer" && value == "exit-after-ready") exit_after_ready = true;
        if (option == "--multiplexer" && value == "never-ready") never_ready = true;
    }
    sigset_t signals;
    ::sigemptyset(&signals);
    ::sigaddset(&signals, SIGTERM);
    if (::sigprocmask(SIG_BLOCK, &signals, nullptr)) return 3;
    auto info = replication::SharedReplicationInfo::Map(fd);
    if (!never_ready) info.get().daemon_ready.store(1, std::memory_order_release);
    if (exit_after_ready) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return 4;
    }
    int signal = 0;
    if (::sigwait(&signals, &signal)) return 5;
    info.get().daemon_ready.store(0, std::memory_order_release);
}
