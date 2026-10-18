#include <server/server.hpp>

#include <spdlog/spdlog.h>
#include <sys/socket.h>

#include <commands.hpp>
#include <resp/receiver.hpp>
#include <resp/resp.hpp>
#include <resp/sender.hpp>
#include <nbio/async.hpp>
#include <nbio/core.hpp>
#include <nbio/time.hpp>
#include <nbio/utility.hpp>
#include <coroutine>
#include <cstdlib>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <backup/backup.hpp>
#include <config/conf_file.hpp>

namespace KV {
namespace detail {
RESP::Object Error(std::string message) { return RESP::Object(RESP::SimpleError{.value = std::move(message)}); }

bool ParseUnsigned(std::string_view text, std::size_t& value) noexcept {
    try {
        std::size_t consumed = 0;
        const auto parsed = std::stoull(std::string(text), &consumed);
        if (consumed != text.size()) {
            return false;
        }
        value = static_cast<std::size_t>(parsed);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}
// The command line is heard first, then the startup file, then the default this
// server would have used on its own. A file that looks as if it was ignored is
// otherwise hard to explain, so the command line says when it overrode one.
template <typename T>
T Resolve(std::string_view name, const std::optional<T>& given, const std::optional<T>& declared, T fallback) {
    if (given) {
        if (declared && !(*declared == *given)) {
            spdlog::warn("config: the file's '{}' is ignored; the command line named one", name);
        }
        return *given;
    }
    return declared ? *declared : std::move(fallback);
}
}  // namespace detail

void Server::run(const ServerOptions& options) {
    std::vector<CommandLine> commands;
    if (options.config_file) {
        commands = ReadCommandFile(*options.config_file);
    }
    const StartupSettings declared = TakeStartupSettings(commands);

    const std::uint16_t port = detail::Resolve("port", options.port, declared.port, ServerOptions::kDefaultPort);
    const std::uint16_t replication_port =
        detail::Resolve("replication-port", options.replication_port, declared.replication_port,
                        ServerOptions::kDefaultReplicationPort);
    const std::string replication_address =
        detail::Resolve("replication-ip", options.replication_address, declared.replication_address,
                        std::string{ServerOptions::kDefaultReplicationAddress});
    const std::string rdma_device =
        detail::Resolve("rdma-device", options.rdma_device, declared.rdma_device, std::string{});

    // What CONFIG GET answers with, and what the client port is bound on.
    port_ = port;
    replication_port_ = replication_port;
    replication_address_ = replication_address;

    const bool keep_log = declared.appendonly.value_or(false);
    const bool has_log = aof_.exists();
    if (keep_log && has_log) {
        if (!aof_.replay([this](const KV::Command& command) { return replay_aof_command(command); })) {
            throw std::runtime_error("failed to load the append-only file");
        }
    } else if (!backup_.load(store_)) {
        throw std::runtime_error("failed to load RDB snapshot");
    }

    ReplicationService::Options replication_options{
        .listen_port = replication_port,
        .listen_address = replication_address,
        .rdma_device = rdma_device,
    };
    ReplicationService::Host host{
        .snapshot_file = [this] { return backup_.path(); },
        .snapshot = [this] { return backup_.save(Backup::capture(store_)); },
        .restore = [this](const std::filesystem::path& file) { return backup_.load_from(file, store_); },
        .apply = [this](const KV::Command& command) { return apply_replicated_command(command); },
        .link_changed =
            [](bool up) { spdlog::info("replication: {}", up ? "the master is up" : "the master went away"); },
    };
    staged_save_ready_ = std::make_unique<nbio::notification::ConditionVariable>();

    if (keep_log && !aof_.enable()) {
        throw std::runtime_error("failed to open the append-only file");
    }

    replication_ = std::make_unique<ReplicationService>(std::move(replication_options), std::move(host));

    if (replication_->is_master()) {
        nbio::async::Spawn(replication_->serve());
    }

    nbio::async::Run(serve(port, std::move(commands)));
}

nbio::async::Task<void> Server::serve(std::uint16_t port, std::vector<CommandLine> commands) {
    // The commands go first: a server that is answering clients is a server that
    // has decided how it is configured.
    co_await apply_commands(std::move(commands));

    try {
        auto address = nbio::net::Address::FromIPv4("0.0.0.0", port);
        if (!address) {
            throw std::system_error(address.error(), "invalid server listen address");
        }
        co_await serve(*address);
    } catch (const std::exception& error) {
        spdlog::error("server stopped: {}", error.what());
        std::exit(EXIT_FAILURE);
    }
}

nbio::async::Task<void> Server::apply_commands(std::vector<CommandLine> commands) {
    for (const CommandLine& line : commands) {
        const KV::CommandValidation validation = KV::ValidateCommand(CommandRequest(line));
        if (!validation) {
            throw std::runtime_error(line.where() + ": " + validation.error);
        }

        const RESP::Object response = co_await execute(*validation.command);
        if (const auto* error = std::get_if<RESP::SimpleError>(&response.value)) {
            throw std::runtime_error(line.where() + ": " + error->value);
        }
        spdlog::info("config: applied '{}'", line.text);
    }

    if (!commands.empty()) {
        spdlog::info("config: {} command(s) applied", commands.size());
    }
}

nbio::async::Task<void> Server::serve(const nbio::net::Address& address) {
    // The listener lives here, in the frame of the loop that uses it, because the
    // accept channel keeps a reference to the listener it waits on.
    nbio::net::TcpListenService listener{address};
    if (auto result = listener.listener().ToggleNoDelay(true); !result) {
        throw std::system_error(result.error(), "setsockopt(TCP_NODELAY) failed");
    }
    co_await accept_clients(listener);
}

nbio::async::Task<void> Server::accept_clients(nbio::net::TcpListenService& listener) {
    while (true) {
        auto accepted = co_await listener.Accept();
        if (!accepted) {
            continue;
        }
        // What accepting hands over is the whole connection: the session that owns it
        // from here, and the address it came from -- which nothing on this side can
        // know, because the other end chose it.
        auto connection = std::move(*accepted);
        nbio::async::Spawn(serve_client(std::make_shared<TcpSessionService>(std::move(connection.first))));
    }
}

namespace {
constexpr std::size_t kReplyBatchBytes = 64U * 1024U;
constexpr std::size_t kCommandsPerTurn = 1024;

struct PipelineYield {
    bool await_ready() const noexcept { return false; }

    template <typename Promise>
    void await_suspend(std::coroutine_handle<Promise> handle) const {
        nbio::runtime::Daemon::scheduler().Submit(nbio::async::Coroutine::FromHandle(handle));
    }

    void await_resume() const noexcept {}
};
}  // namespace

nbio::async::Task<void> Server::serve_client(std::shared_ptr<TcpSessionService> session) {
    // A pipeline can hold more than one command, and a single command can be
    // larger than a socket read, so the receive buffer starts roomy and is
    // allowed to grow: the decoder needs the whole command before it can hand
    // one over.
    auto recv_buffer = std::make_unique<::nbio::utility::Buffer>(256U * 1024U, 16U * 1024U * 1024U);
    auto send_buffer = std::make_unique<::nbio::utility::Buffer>(16U * 1024U, 16U * 1024U * 1024U);

    RESP::Receiver receiver(session->transport(), *recv_buffer);
    while (true) {
        auto command = co_await receiver.ReceiveCommand();
        if (receiver.no_command()) {
            // A line with no command on it: nothing is owed to the client for
            // it, and the connection stays up for the command that follows.
            continue;
        }
        if (!command) {
            RESP::Sender sender(session->transport(), *send_buffer);
            if (!receiver.decode_error().empty()) {
                // The bytes that did not parse are already dropped, so the
                // commands behind them are still decodable: the client is told
                // and the connection goes on.
                (void)co_await sender.Send(RESP::Object(RESP::SimpleError{.value = receiver.decode_error()}));
                continue;
            }
            (void)co_await sender.Send(RESP::Object(RESP::SimpleError{.value = "internal error"}));
            co_return;
        }

        RESP::Sender sender(session->transport(), *send_buffer);
        std::size_t commands_this_turn = 0;
        while (true) {
            if (session->is_multi || command->object || !answer_borrowed(command->words, *send_buffer)) {
                std::optional<RESP::Object> answered;
                KV::CommandValidation validation =
                    command->object ? KV::ValidateCommand(*command->object) : KV::ValidateCommand(command->words);
                if (validation) {
                    if (!session->is_multi) {
                        answered = execute_fast(*validation.command);
                    }
                    if (!answered) {
                        answered = co_await dispatch(*session, std::move(*validation.command));
                    }
                } else {
                    answered = detail::Error(validation.error);
                }
                sender.Append(*answered);
            }
            if (sender.Pending() >= kReplyBatchBytes) {
                if (!co_await sender.Flush()) {
                    co_return;
                }
            }
            if (++commands_this_turn == kCommandsPerTurn) {
                commands_this_turn = 0;
                co_await PipelineYield{};
            }

            auto next = receiver.TryReceiveCommand();
            if (next) {
                command = std::move(next);
                continue;
            }
            if (!receiver.decode_error().empty()) {
                sender.Append(RESP::Object(RESP::SimpleError{.value = receiver.decode_error()}));
            }
            break;
        }
        if (!co_await sender.Flush()) {
            co_return;
        }
    }
}

bool Server::answer_borrowed(std::span<const std::string_view> words, nbio::utility::Buffer& buffer) {
    bool appended = false;
    if (words.size() == 2 && KV::IsCommandName(words.front(), KV::CommandType::kGet)) {
        const std::string* value = store_.get_ref(words[1]);
        appended = RESP::AppendBulkString(value ? std::optional<std::string_view>(*value) : std::nullopt, buffer);
    } else if (words.size() == 3 && KV::IsCommandName(words.front(), KV::CommandType::kSet)) {
        if (replica_read_only_) {
            appended = RESP::AppendObject(detail::Error("READONLY You can't write against a read only replica."), buffer);
        } else {
            if (aof_.enabled() || (replication_ && replication_->has_replicas())) {
                return false;
            }
            if (store_.set(words[1], words[2])) {
                note_write();
                appended = RESP::AppendObject(RESP::Object(RESP::SimpleString{.value = "OK"}), buffer);
            } else {
                appended = RESP::AppendObject(detail::Error("OOM command not allowed when eviction_policy is noeviction"), buffer);
            }
        }
    } else {
        return false;
    }
    if (!appended) {
        throw std::runtime_error("reply batch is larger than the send buffer can hold");
    }
    return true;
}

nbio::async::Task<RESP::Object> Server::dispatch(TcpSessionService& session, KV::Command command) {
    if (command.type == KV::CommandType::kMulti) {
        if (session.is_multi) {
            co_return detail::Error("ERR MULTI calls can not be nested");
        }
        session.is_multi = true;
        co_return RESP::Object(RESP::SimpleString{.value = "OK"});
    }
    if (command.type == KV::CommandType::kExec) {
        if (!session.is_multi) {
            co_return detail::Error("ERR EXEC without MULTI");
        }
        session.is_multi = false;
        RESP::Array results;
        results.values.reserve(session.queued_commands.size());
        for (const KV::Command& queued : session.queued_commands) {
            results.values.push_back(co_await execute(queued));
        }
        session.queued_commands.clear();
        co_return RESP::Object(std::move(results));
    }
    if (session.is_multi) {
        session.queued_commands.push_back(std::move(command));
        co_return RESP::Object(RESP::SimpleString{.value = "QUEUED"});
    }
    co_return co_await execute(command);
}

std::optional<RESP::Object> Server::execute_fast(const KV::Command& command) {
    switch (command.type) {
        case KV::CommandType::kPing:
            return execute_ping(command);
        case KV::CommandType::kGet:
            return execute_get(command);
        case KV::CommandType::kSet: {
            if (replica_read_only_) {
                return detail::Error("READONLY You can't write against a read only replica.");
            }
            if (aof_.enabled()) {
                return std::nullopt;
            }
            RESP::Object response = execute_set(command);
            if (!std::holds_alternative<RESP::SimpleError>(response.value) && replication_ != nullptr) {
                replication_->record(command);
            }
            return response;
        }
        default:
            return std::nullopt;
    }
}

nbio::async::Task<RESP::Object> Server::execute(const KV::Command& command) {
    if (replica_read_only_ && KV::IsWriteCommand(command.type)) {
        co_return detail::Error("READONLY You can't write against a read only replica.");
    }

    std::optional<RESP::Object> response;
    switch (command.type) {
        case KV::CommandType::kPing:
            response.emplace(execute_ping(command));
            break;
        case KV::CommandType::kInfo:
            response.emplace(co_await execute_info(command));
            break;
        case KV::CommandType::kGet:
            response.emplace(execute_get(command));
            break;
            {
                (void)command;
                co_return RESP::Object(RESP::SimpleString{.value = "KVStore"});
            }
        case KV::CommandType::kSet:
            response.emplace(execute_set(command));
            break;
        case KV::CommandType::kDel:
            response.emplace(co_await execute_del(command));
            break;
        case KV::CommandType::kExists:
            response.emplace(co_await execute_exists(command));
            break;
        case KV::CommandType::kDbSize:
            response.emplace(co_await execute_dbsize(command));
            break;
        case KV::CommandType::kCommand:
            response.emplace(co_await execute_command_info(command));
            break;
        case KV::CommandType::kClient:
            response.emplace(co_await execute_client(command));
            break;
        case KV::CommandType::kConfig:
            response.emplace(co_await execute_config(command));
            break;
        case KV::CommandType::kBgSave:
            response.emplace(co_await execute_bgsave(command));
            break;
        case KV::CommandType::kSave:
            response.emplace(co_await execute_save(command));
            break;
        case KV::CommandType::kSlaveOf:
            response.emplace(co_await execute_slaveof(command));
            break;
        default:
            response.emplace(detail::Error("ERR command cannot be executed"));
            break;
    }

    // The same writes the AOF records are the ones a replica has to be told
    // about, so they go into the replication log too -- and they go in first.
    // The store has already been changed by the time this runs, so the log has
    // to hold the write before this frame awaits: a replica that is handed a
    // snapshot across that await would otherwise be given the write in its image
    // and then read it again from the log. Recording never waits itself.
    if (KV::IsWriteCommand(command.type) && !std::holds_alternative<RESP::SimpleError>(response->value)) {
        if (replication_ != nullptr) {
            replication_->record(command);
        }
        if (aof_.enabled()) {
            co_await aof_.append(command);
        }
    }
    co_return std::move(*response);
}

RESP::Object Server::execute_ping(const KV::Command& command) {
    (void)command;
    return RESP::Object(RESP::SimpleString{.value = "PONG"});
}

RESP::Object Server::execute_get(const KV::Command& command) {
    const auto& get = std::get<KV::GetParams>(command.parameters);
    return RESP::Object(RESP::BulkString{.value = store_.get(get.key)});
}

RESP::Object Server::execute_set(const KV::Command& command) {
    const auto& set = std::get<KV::SetParams>(command.parameters);
    if (!store_.set(set.key, set.value)) {
        return detail::Error("OOM command not allowed when eviction_policy is noeviction");
    }
    note_write();
    return RESP::Object(RESP::SimpleString{.value = "OK"});
}

nbio::async::Task<RESP::Object> Server::execute_del(const KV::Command& command) {
    const auto& del = std::get<KV::DelParams>(command.parameters);
    const bool exists = store_.erase(del.key);
    note_write();
    co_return RESP::Object(RESP::Integer{.value = exists ? 1 : 0});
}

nbio::async::Task<RESP::Object> Server::execute_exists(const KV::Command& command) {
    const auto& exists = std::get<KV::ExistsParams>(command.parameters);
    co_return RESP::Object(RESP::Boolean{.value = store_.contains(exists.key)});
}

nbio::async::Task<RESP::Object> Server::execute_dbsize(const KV::Command& command) {
    (void)command;
    // The count in the index, like Redis, and not a walk that would also drop the
    // keys whose TTL has passed on the way -- a size query does not get to cost
    // time proportional to the store. A key that has expired but has not been
    // visited yet is therefore still counted, which is also what Redis does.
    co_return RESP::Object(RESP::Integer{.value = static_cast<std::int64_t>(store_.size())});
}

// `SLAVEOF <ip> <port>`, the one command that changes what this instance is:
// from here it is a replica, and a replica refuses its own clients' writes. The
// address is the master's rdma address, not the TCP one it answers clients on.
nbio::async::Task<RESP::Object> Server::execute_slaveof(const KV::Command& command) {
    const auto& slaveof = std::get<KV::SlaveOfParams>(command.parameters);

    if (replication_ == nullptr) [[unlikely]] {
        co_return RESP::Object(RESP::SimpleError{.value = "ERR replication is not available on this server"});
    }
    if (replication_->is_replica()) {
        co_return RESP::Object(RESP::SimpleError{.value = "ERR this instance already follows a master"});
    }

    const auto master = nbio::net::Address::FromIPv4(slaveof.address, slaveof.port);
    if (!master) {
        co_return RESP::Object(RESP::SimpleError{.value = "ERR invalid SLAVEOF address"});
    }
    if (!replication_->slave_of(*master)) [[unlikely]] {
        // The one thing that can stop it here is the device, and the service has
        // already said which option names it.
        co_return RESP::Object(
            RESP::SimpleError{.value = "ERR replication needs an rdma device; start the server with one"});
    }

    // Read-only from here, whether or not the link is up yet: what this
    // instance's clients may write does not depend on the state of a socket, and
    // the store it is about to replace does not belong to them any more.
    replica_read_only_ = true;
    co_return RESP::Object(RESP::SimpleString{.value = "OK"});
}

bool Server::replay_aof_command(const KV::Command& command) {
    switch (command.type) {
        case KV::CommandType::kSet: {
            const auto& set = std::get<KV::SetParams>(command.parameters);
            store_.set(set.key, set.value);
            return true;
        }
        case KV::CommandType::kDel: {
            const auto& del = std::get<KV::DelParams>(command.parameters);
            store_.erase(del.key);
            return true;
        }
        case KV::CommandType::kExpire:
            return false;
        default:
            return true;
    }
}

bool Server::apply_replicated_command(const KV::Command& command) {
    // The same mutations the AOF replay knows, and for the same reason: these are
    // the writes a master applies, and this end is applying the ones its master
    // applied, in the order it applied them. Recording them is what lets a replica
    // of this replica be caught up from the same bytes.
    switch (command.type) {
        case KV::CommandType::kSet: {
            const auto& set = std::get<KV::SetParams>(command.parameters);
            if (!store_.set(set.key, set.value)) {
                return false;
            }
            note_write();
            break;
        }
        case KV::CommandType::kDel: {
            const auto& del = std::get<KV::DelParams>(command.parameters);
            store_.erase(del.key);
            note_write();
            break;
        }
        default:
            break;
    }

    if (replication_ != nullptr) {
        replication_->record(command);
    }
    return true;
}

std::optional<std::string> Server::config_value(const std::string& parameter) const {
    if (parameter == "appendonly") {
        return aof_.enabled() ? "yes" : "no";
    }
    if (parameter == "aof_checksum") {
        return aof_.checksum() ? "yes" : "no";
    }
    if (parameter == "eviction_policy") {
        return store_.eviction_policy() == KV::EvictionPolicy::kLRU ? "lru" : "noeviction";
    }
    if (parameter == "appendfsync") {
        // The AOF is written with pwrite and never synced, which is exactly what
        // Redis calls `appendfsync no`.
        return "no";
    }
    if (parameter == "save") {
        if (!staged_save_rule_) {
            return "";
        }
        return std::to_string(staged_save_rule_->seconds.count()) + " " + std::to_string(staged_save_rule_->changed);
    }
    if (parameter == "port") {
        return std::to_string(port_);
    }
    if (parameter == "rdma_device") {
        return rdma_device_;
    }
    if (parameter == "replication_address") {
        // The address and port a replica connects to, or nothing when this server
        // serves no replicas.
        return replication_port_ == 0 ? std::string{} : replication_address_ + " " + std::to_string(replication_port_);
    }
    return std::nullopt;
}

nbio::async::Task<RESP::Object> Server::execute_command_info(const KV::Command& command) {
    (void)command;
    // No command metadata to publish. An empty array is a well-formed answer,
    // which is all redis-cli needs to stop reporting the probe as an error.
    co_return RESP::Object(RESP::Array{});
}

nbio::async::Task<RESP::Object> Server::execute_info(const KV::Command& command) {
    (void)command;
    co_return RESP::Object(RESP::SimpleString{.value = "KVStore"});
}

nbio::async::Task<RESP::Object> Server::execute_client(const KV::Command& command) {
    const auto& client = std::get<KV::ClientParams>(command.parameters);

    // The handshake subcommands have to succeed. A client announces itself with
    // `CLIENT SETINFO` the moment it connects, and an error there makes it treat
    // the connection as broken before it ever sends a command. Nothing is kept:
    // this server has no per-connection state to name.
    if (client.subcommand == "setinfo" || client.subcommand == "setname" || client.subcommand == "no-evict" ||
        client.subcommand == "no-touch") {
        co_return RESP::Object(RESP::SimpleString{.value = "OK"});
    }
    if (client.subcommand == "getname") {
        co_return RESP::Object(RESP::BulkString{.value = std::string{}});
    }
    if (client.subcommand == "id") {
        co_return RESP::Object(RESP::Integer{.value = 0});
    }

    // CLIENT INFO and CLIENT LIST describe connections this server does not
    // track, so an empty description is the honest answer.
    co_return RESP::Object(RESP::BulkString{.value = std::string{}});
}

nbio::async::Task<RESP::Object> Server::execute_config(const KV::Command& command) {
    const auto& config = std::get<KV::ConfigParams>(command.parameters);

    if (config.values.empty()) {
        // CONFIG GET answers with a flat name/value array, and an unknown name is
        // an empty array rather than an error -- the same as Redis.
        const std::vector<std::string> names =
            config.parameter == "*" ? std::vector<std::string>{"appendonly", "aof_checksum", "appendfsync",
                                                               "eviction_policy", "save", "port", "replication_address"}
                                    : std::vector<std::string>{config.parameter};
        std::vector<RESP::Object> values;
        for (const std::string& name : names) {
            if (const auto value = config_value(name)) {
                values.push_back(RESP::Object(RESP::BulkString{.value = name}));
                values.push_back(RESP::Object(RESP::BulkString{.value = *value}));
            }
        }
        co_return RESP::Object(RESP::Array{.values = std::move(values)});
    }

    if (config.parameter == "appendonly") {
        if (config.values.front() == "yes") {
            if (!aof_.enable()) {
                co_return detail::Error("ERR CONFIG SET failed - could not open the AOF");
            }
        } else {
            aof_.disable();
        }
    } else if (config.parameter == "aof_checksum") {
        // The setting decides what is written from here on and nothing else: a log is
        // read for whatever it holds, so toggling this never makes an existing log
        // unreadable.
        aof_.checksum(config.values.front() == "yes");
    } else if (config.parameter == "eviction_policy") {
        store_.set_eviction_policy(config.values.front() == "lru" ? KV::EvictionPolicy::kLRU
                                                                  : KV::EvictionPolicy::kNoEviction);
    } else if (config.parameter == "save") {
        if (config.values.size() != 2) {
            co_return detail::Error("ERR CONFIG SET failed - 'save' wants <seconds> <changed>");
        }

        std::size_t seconds = 0;
        std::size_t changed = 0;
        if (!detail::ParseUnsigned(config.values.front(), seconds) ||
            !detail::ParseUnsigned(config.values.back(), changed) || changed == 0) {
            co_return detail::Error("ERR CONFIG SET failed - 'save' wants <seconds> <changed>, with changed > 0");
        }
        configure_staged_save(std::chrono::seconds{seconds}, changed);
    } else if (KV::IsStartupConfigParameter(config.parameter)) {
        // The ports are bound and the replication listener is built while the
        // server is being put together, so a running one has nothing left to
        // apply this to: it belongs in a command file, not in a command.
        co_return detail::Error("ERR CONFIG SET failed - '" + config.parameter +
                                "' is a startup setting; it can only be named in the command file");
    }
    co_return RESP::Object(RESP::SimpleString{.value = "OK"});
}

nbio::async::Task<RESP::Object> Server::execute_bgsave(const KV::Command& command) {
    (void)command;
    if (!co_await backup_.save(store_)) {
        co_return detail::Error("ERR failed to save RDB snapshot");
    }
    staged_save_dirty_ = 0;
    co_return RESP::Object(RESP::SimpleString{.value = "Background saving started"});
}

nbio::async::Task<RESP::Object> Server::execute_save(const KV::Command& command) {
    (void)command;
    // The image is written in this process, so this frame's thread -- the one the
    // event loop runs on -- is busy until the file is in place. That is what SAVE
    // is for: the snapshot on disk before the answer, with the server standing
    // still in the meantime, where BGSAVE hands the writing to a child.
    if (!backup_.save_now(store_)) {
        co_return detail::Error("ERR failed to save RDB snapshot");
    }
    staged_save_dirty_ = 0;
    co_return RESP::Object(RESP::SimpleString{.value = "OK"});
}

void Server::note_write() {
    if (!staged_save_rule_) {
        return;
    }

    ++staged_save_dirty_;
    if (staged_save_dirty_ == staged_save_rule_->changed) {
        staged_save_ready_->NotifyOne();
    }
}

void Server::configure_staged_save(std::chrono::seconds seconds, std::size_t changed) {
    staged_save_rule_ = StagedSaveRule{.seconds = seconds, .changed = changed};
    staged_save_dirty_ = 0;
    maybe_start_staged_save();
}

void Server::maybe_start_staged_save() {
    if (!staged_save_rule_ || staged_save_loop_running_) {
        return;
    }

    staged_save_loop_running_ = true;
    nbio::async::Spawn(staged_save_periodic());
}

nbio::async::Task<void> Server::staged_save_periodic() {
    while (staged_save_rule_) {
        const auto rule = *staged_save_rule_;
        auto timeout = nbio::time::SystemTimeService{}.Sleep(rule.seconds);
        auto ready = staged_save_ready_->wait(
            [this, expected = rule.changed] { return !staged_save_rule_ || staged_save_dirty_ >= expected; });
        co_await nbio::async::WhenAll(std::move(timeout), std::move(ready));

        if (!staged_save_rule_) {
            break;
        }

        if (staged_save_dirty_ < rule.changed) {
            // The rule changed or the save was disabled while we were waiting.
            continue;
        }

        const bool ok = co_await backup_.save(store_);
        if (!ok) {
            spdlog::warn("staged save failed");
            continue;
        }

        if (staged_save_dirty_ > rule.changed) {
            staged_save_dirty_ -= rule.changed;
        } else {
            staged_save_dirty_ = 0;
        }
    }

    staged_save_loop_running_ = false;
}
}  // namespace KV
