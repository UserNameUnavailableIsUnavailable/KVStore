#include <replication/repld_process.hpp>

#include <spawn.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <vector>

extern char** environ;

namespace replication {
RepldProcess::RepldProcess(SharedReplicationInfo& info, const std::string& executable,
                           const std::string& address, unsigned port, const std::string& backend) {
    const auto server = std::filesystem::read_symlink("/proc/self/exe");
    const auto binary = executable.empty() ? server.parent_path() / "repld" : std::filesystem::path(executable);
    std::vector<std::string> args{binary.string(), "--pid", std::to_string(::getpid()),
        "--binary", server.string(), "--info-fd", "3", "--listen-address", address,
        "--port", std::to_string(port), "--multiplexer", backend};
    std::vector<char*> argv;
    for (auto& arg : args) argv.push_back(arg.data());
    argv.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    int error = ::posix_spawn_file_actions_init(&actions);
    if (error) throw std::system_error(error, std::generic_category(), "preparing repld spawn");
    error = ::posix_spawn_file_actions_adddup2(&actions, info.descriptor(), 3);
    if (!error) error = ::posix_spawn(&pid_, binary.c_str(), &actions, nullptr, argv.data(), environ);
    ::posix_spawn_file_actions_destroy(&actions);
    if (error) {
        pid_ = -1;
        throw std::system_error(error, std::generic_category(), "starting repld");
    }
    try {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!info.get().daemon_ready.load(std::memory_order_acquire)) {
            Check();
            if (std::chrono::steady_clock::now() >= deadline) throw std::runtime_error("repld startup timed out");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        Check();
    } catch (...) {
        Stop();
        throw;
    }
}

RepldProcess::~RepldProcess() { Stop(); }

void RepldProcess::Check() {
    if (pid_ < 0) throw std::runtime_error("repld is not running");
    int status = 0;
    const auto result = ::waitpid(pid_, &status, WNOHANG);
    if (result == 0 || (result < 0 && errno == EINTR)) return;
    if (result < 0) {
        const auto error = errno;
        if (error == ECHILD) pid_ = -1;
        throw std::system_error(error, std::generic_category(), "checking repld");
    }
    pid_ = -1;
    throw std::runtime_error("repld exited unexpectedly (wait status " + std::to_string(status) + ")");
}

void RepldProcess::Stop() noexcept {
    if (pid_ < 0) return;
    // A child killed by the OS is still ours until reaped, so its PID cannot be reused.
    if (::kill(pid_, SIGTERM) && errno != ESRCH) std::cerr << "replication: cannot terminate repld\n";
    int status;
    for (int attempt = 0; attempt < 100; ++attempt) {
        const auto result = ::waitpid(pid_, &status, WNOHANG);
        if (result == pid_ || (result < 0 && errno == ECHILD)) { pid_ = -1; return; }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (::kill(pid_, SIGKILL) && errno != ESRCH) std::cerr << "replication: cannot kill unresponsive repld\n";
    while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {}
    pid_ = -1;
}
}  // namespace replication
