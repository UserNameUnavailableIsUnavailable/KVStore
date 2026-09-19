#pragma once

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
#include "Store.hpp"
#include "ReplicationService.hpp"

#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/Core/Address.hpp>
#include <Foundation/NBIO/AcceptChannel.hpp>
#include <Foundation/NBIO/Session.hpp>
#include <Foundation/Async/Task.hpp>

#include <Application/RESP/RESP.hpp>
#include <Application/Commands.hpp>

namespace KV
{
// What a server is told to do, from the command line and from a startup file.
// Nothing here means "not named": a value is resolved as the command line, then
// the file, then the default, so a flag overrides the file only when it was
// actually given. The defaults describe a plain stand-alone instance: no
// replication listener, and no master to follow.
struct ServerOptions
{
    static constexpr std::uint16_t kDefaultPort = 8080;
    static constexpr std::uint16_t kDefaultReplicationPort = 0;
    static constexpr std::string_view kDefaultReplicationAddress = "0.0.0.0";

    // The TCP port clients connect to.
    std::optional<std::uint16_t> port{};
    // 0 leaves the replication listener off.
    std::optional<std::uint16_t> replication_port{};
    std::optional<std::string> replication_address{};
    // Set to make this instance a read-only replica of that master.
    std::optional<Foundation::Core::Address> master{};
    // The event multiplexer that drives this server instance.
    std::string multiplexer{"epoll"};
    // The startup command file, if one was given: each line is a command, applied
    // exactly as a client's would be, apart from the lines that are settings.
    std::optional<std::filesystem::path> config_file{};
};

class Session
{
  public:
    explicit Session(std::shared_ptr<Foundation::NBIO::Session> transport) : transport_(std::move(transport))
    {
    }

    Foundation::NBIO::Session &transport() const noexcept
    {
        return *transport_;
    }

    bool is_multi{false};
    std::vector<KV::Command> queued_commands;
    // The key of a single-key read, copied here rather than into a string of its
    // own: the connection reuses this buffer for every lookup, so the key of a
    // GET costs no allocation once it has grown to the longest one seen.
    std::string lookup_key;

  private:
    std::shared_ptr<Foundation::NBIO::Session> transport_;
};

class Server
{
  public:
    Server() = default;
    ~Server() = default;
    Server(const Server &) = delete;
    Server &operator=(const Server &) = delete;
    Server(Server &&) = delete;
    Server &operator=(Server &&) = delete;

    void run(const ServerOptions &options);

    // Runs the commands a startup file holds, and then serves. Applying them
    // belongs inside the runtime: they are this server's own commands, and
    // executing one can await.
    Foundation::NBIO::Task<void> start(std::uint16_t port, std::vector<CommandLine> commands);
    Foundation::NBIO::Task<void> apply_commands(std::vector<CommandLine> commands);

    Foundation::NBIO::Task<void> serve(const Foundation::Core::Address &address);
    Foundation::NBIO::Task<void> accept_clients(std::unique_ptr<Foundation::NBIO::AcceptChannel> listener);
    Foundation::NBIO::Task<void> serve_client(std::shared_ptr<Session> session);

  private:
    // The answer to a command that reads one key, or nothing when the request is
    // not one: see the definition for why it is worth answering before a command
    // is built for it.
    [[nodiscard]] std::optional<RESP::Object> answer_read(std::span<const std::string_view> words, Session &session);

    Foundation::NBIO::Task<RESP::Object> dispatch(Session &session, KV::Command command);
    Foundation::NBIO::Task<RESP::Object> execute(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_ping(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_get(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_set(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_info(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_del(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_exists(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_dbsize(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_command_info(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_client(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_config(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_bgsave(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_save(const KV::Command &command);

    // The current value of one CONFIG parameter, or nothing when this server
    // does not know the name.
    std::optional<std::string> config_value(const std::string &parameter) const;

    bool replay_aof_command(const KV::Command &command);
    // Applies one command a master has applied. A replica refuses writes from its
    // own clients, so this is the write path without that check: it is the master
    // that said this write happened, in this order.
    bool apply_replicated_command(const KV::Command &command);

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
};
} // namespace KV
