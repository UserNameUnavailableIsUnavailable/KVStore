#pragma once

#include <nbio/async.hpp>
#include <nbio/net/tcp_listen_service.hpp>
#include <nbio/notification/condition_variable.hpp>
#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <cstdint>
#include <vector>
#include <replication/replication_info.hpp>

namespace replication {
struct DaemonOptions {
    int pid = 0;
    int info_fd = -1;
    std::string binary;
    std::string bpf_object;
    std::string listen_address = "127.0.0.1";
    std::uint16_t port = 8081;
    std::size_t backlog_bytes = 64U * 1024U * 1024U;
    std::string multiplexer = "epoll";
};

int RunDaemon(const DaemonOptions& options);
int DaemonMain(int argc, char** argv);

class ReplayBacklog;

class ReplicationDaemon {
public:
    ReplicationDaemon(ReplayBacklog& backlog, ReplicationInfo& info);
    ~ReplicationDaemon();
    ReplicationDaemon(const ReplicationDaemon&) = delete;
    ReplicationDaemon& operator=(const ReplicationDaemon&) = delete;

    nbio::async::Task<void> Serve(nbio::net::Address address);
    void Maintain();
    void Stop();

private:
    struct Client;
    nbio::async::Task<void> Follow(std::shared_ptr<Client> client);
    nbio::async::Task<void> Write(std::shared_ptr<Client> client);
    static nbio::async::Task<void> ReadPeer(std::shared_ptr<Client> client);
    static nbio::async::Task<bool> Send(std::shared_ptr<Client> client, std::string_view bytes);

    ReplayBacklog& backlog_;
    ReplicationInfo& info_;
    nbio::notification::ConditionVariable changed_;
    std::vector<std::shared_ptr<Client>> clients_;
    std::vector<nbio::async::CoroutineJoinHandle> followers_;
    std::uint64_t observed_head_ = 0;
    std::uint64_t observed_generation_ = 0;
    std::chrono::steady_clock::time_point heartbeat_{};
    bool stopping_ = false;
};
}  // namespace replication
