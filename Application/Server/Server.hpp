#pragma once

#include <Application/Commands.hpp>
#include <Application/RESP/RESP.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Net/SocketAddress.hpp>
#include <NBIO/Notification/ConditionVariable.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/Net/TcpAcceptService.hpp>
#include <NBIO/Net/TcpSessionService.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "AppendOnlyFile.hpp"
#include "Backup.hpp"
#include "ConfFile.hpp"
#include "ReplicationService.hpp"
#include "Store.hpp"

namespace KV {
// What a server is told to do, from the command line and from a startup file.
// Nothing here means "not named": a value is resolved as the command line, then
// the file, then the default, so a flag overrides the file only when it was
// actually given. The defaults describe a plain stand-alone instance: no
// replication listener, and no master to follow -- which one it follows is what
// SLAVEOF says, and it says it at runtime.
struct ServerOptions {
    static constexpr std::uint16_t kDefaultPort = 8080;
    static constexpr std::uint16_t kDefaultReplicationPort = 0;
    static constexpr std::string_view kDefaultReplicationSocketAddress = "0.0.0.0";

    // The TCP port clients connect to.
    std::optional<std::uint16_t> port{};
    // 0 leaves the replication listener off.
    std::optional<std::uint16_t> replication_port{};
    std::optional<std::string> replication_address{};
    // The RDMA device replication runs on. Serving replicas needs it, and so
    // does following a master, which SLAVEOF asks for after this is settled.
    std::optional<std::string> rdma_device{};
    // The event multiplexer that drives this server instance.
    std::string multiplexer{"epoll"};
    // The startup command file, if one was given: each line is a command, applied
    // exactly as a client's would be, apart from the lines that are settings.
    std::optional<std::filesystem::path> config_file{};
};

class TcpSessionService {
   public:
    explicit TcpSessionService(std::shared_ptr<NBIO::Net::TcpSessionService> transport)
        : transport_(std::move(transport)) {}

    NBIO::Net::TcpSessionService& transport() const noexcept { return *transport_; }

    bool is_multi{false};
    std::vector<KV::Command> queued_commands;
    // The key of a single-key read, copied here rather than into a string of its
    // own: the connection reuses this buffer for every lookup, so the key of a
    // GET costs no allocation once it has grown to the longest one seen.
    std::string lookup_key;

   private:
    std::shared_ptr<NBIO::Net::TcpSessionService> transport_;
};

class Server {
   public:
    Server() = default;
    ~Server() = default;
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) = delete;
    Server& operator=(Server&&) = delete;

    void run(const ServerOptions& options);

    // Runs the commands a startup file holds, and then serves. Applying them
    // belongs inside the runtime: they are this server's own commands, and
    // executing one can await.
    NBIO::Async::Task<NBIO::Runtime, void> serve(std::uint16_t port, std::vector<CommandLine> commands);
    NBIO::Async::Task<NBIO::Runtime, void> apply_commands(std::vector<CommandLine> commands);

    NBIO::Async::Task<NBIO::Runtime, void> serve(const NBIO::Net::SocketAddress& address);
    // The listener is the caller's: it owns the acceptor the channel waits on, so it
    // has to outlive the loop that accepts through it.
    NBIO::Async::Task<NBIO::Runtime, void> accept_clients(NBIO::Net::TcpAcceptService& listener);
    NBIO::Async::Task<NBIO::Runtime, void> serve_client(std::shared_ptr<TcpSessionService> session);

   private:
    struct StagedSaveRule {
        std::chrono::seconds seconds{0};
        std::size_t changed{0};
    };

    // The answer to a command that reads one key, or nothing when the request is
    // not one: see the definition for why it is worth answering before a command
    // is built for it.
    [[nodiscard]] std::optional<RESP::Object> answer_read(std::span<const std::string_view> words,
                                                          TcpSessionService& session);

    NBIO::Async::Task<NBIO::Runtime, RESP::Object> dispatch(TcpSessionService& session, KV::Command command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_ping(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_get(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_set(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_info(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_del(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_exists(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_dbsize(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_command_info(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_client(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_config(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_bgsave(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_save(const KV::Command& command);
    NBIO::Async::Task<NBIO::Runtime, RESP::Object> execute_slaveof(const KV::Command& command);
    void note_write();
    void configure_staged_save(std::chrono::seconds seconds, std::size_t changed);
    void maybe_start_staged_save();
    NBIO::Async::Task<NBIO::Runtime, void> staged_save_periodic();

    // The current value of one CONFIG parameter, or nothing when this server
    // does not know the name.
    std::optional<std::string> config_value(const std::string& parameter) const;

    bool replay_aof_command(const KV::Command& command);
    // Applies one command a master has applied. A replica refuses writes from its
    // own clients, so this is the write path without that check: it is the master
    // that said this write happened, in this order.
    bool apply_replicated_command(const KV::Command& command);

    LRUStore<std::string, std::string, HashMap> store_;
    AppendOnlyFile aof_;
    Backup backup_;
    // The replication service this server plugs in. It is built in run(), once
    // the store exists and the command line is known.
    std::unique_ptr<ReplicationService> replication_;
    // A replica refuses writes: it is not the source of truth, its master is.
    bool replica_read_only_{false};
    // What this server decided to listen on. CONFIG GET answers with it, and it
    // is the one place a setting that was already settled can be read back from.
    std::uint16_t port_{0};
    std::uint16_t replication_port_{0};
    std::string replication_address_{};
    std::string rdma_device_{};

    std::optional<StagedSaveRule> staged_save_rule_{};
    std::size_t staged_save_dirty_{0};
    std::unique_ptr<NBIO::Notification::ConditionVariable> staged_save_ready_{};
    bool staged_save_loop_running_{false};
};
}  // namespace KV
