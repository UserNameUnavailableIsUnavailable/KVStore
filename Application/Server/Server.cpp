#include "Server.hpp"
#include "ConfFile.hpp"
#include <Foundation/NBIO/EpollMultiplexer.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/NBIO.hpp>
#include "Backup.hpp"

#include <Foundation/Async/Async.hpp>
#include <Foundation/Core/Buffer.hpp>

#include <Application/Commands.hpp>
#include <Application/RESP/RESP.hpp>
#include <Application/RESP/Receiver.hpp>
#include <Application/RESP/Sender.hpp>


#include <Foundation/NBIO/Types.hpp>
#include <Foundation/NBIO/URingMultiplexer.hpp>
#include <cstdlib>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>
#include <stdexcept>
#include <sys/socket.h>
#include <utility>
#include <vector>

namespace KV
{
namespace detail
{
RESP::Object Error(std::string message)
{
    return RESP::Object(RESP::SimpleError{.value = std::move(message)});
}
// The command line is heard first, then the startup file, then the default this
// server would have used on its own. A file that looks as if it was ignored is
// otherwise hard to explain, so the command line says when it overrode one.
template <typename T>
T Resolve(std::string_view name, const std::optional<T> &given, const std::optional<T> &declared, T fallback)
{
    if (given)
    {
        if (declared && !(*declared == *given))
        {
            spdlog::warn("config: the file's '{}' is ignored; the command line named one", name);
        }
        return *given;
    }
    return declared ? *declared : std::move(fallback);
}} // namespace

void Server::run(const ServerOptions &options)
{
    // The file is read before anything is built, because some of its lines do not
    // wait for the server to exist: the ports it listens on and the master it
    // follows are settled here, before a socket or the replication service is
    // created. What is left of the file is commands, and they are run once the
    // server is up.
    std::vector<CommandLine> commands;
    if (options.config_file)
    {
        commands = ReadCommandFile(*options.config_file);
    }
    const StartupSettings declared = TakeStartupSettings(commands);

    const std::uint16_t port = detail::Resolve("port", options.port, declared.port, ServerOptions::kDefaultPort);
    const std::uint16_t replication_port = detail::Resolve("replication-port", options.replication_port,
                                                           declared.replication_port, ServerOptions::kDefaultReplicationPort);
    const std::string replication_address =
        detail::Resolve("replication-address", options.replication_address, declared.replication_address,
                        std::string{ServerOptions::kDefaultReplicationAddress});

    // Whether this instance is a replica is part of the same settling, so a flag
    // and a file line that both name a master would be a question rather than a
    // setting: the command line is the answer.
    const std::optional<Foundation::Core::Address> master = options.master ? options.master : declared.master;
    if (options.master && declared.master)
    {
        spdlog::warn("config: the file's 'replicaof' is ignored; the command line named a master");
    }

    // What CONFIG GET answers with, and what the client port is bound on.
    port_ = port;
    replication_port_ = replication_port;
    replication_address_ = replication_address;

    if (!backup_.load(store_))
    {
        throw std::runtime_error("failed to load RDB snapshot");
    }
    if (!aof_.replay([this](const KV::Command &command) {
            return replay_aof_command(command);
        }))
    {
        throw std::runtime_error("failed to load AOF snapshot");
    }

    // Replication is a service this server plugs in: the service owns the
    // replication port and everything on the wire, the server owns the store it
    // snapshots and restores. A replica is read-only from the moment it is told
    // to follow a master, whether or not the link is up yet.
    replica_read_only_ = master.has_value();

    ReplicationService::Options replication_options{
        .listen_port = replication_port,
        .listen_address = replication_address,
        .master = master,
    };
    ReplicationService::Host host{
        .snapshot_file = [this] {
            return backup_.path();
        },
        .snapshot = [this] {
            // capture() copies the store here, in this step, so the write log
            // starts from exactly what the snapshot holds and nothing can fall
            // between the two. The task it hands back does the fork and the
            // write.
            return backup_.save(Backup::capture(store_));
        },
        .restore = [this](const std::filesystem::path &file) {
            // Loading the received RDB is also what verifies it: the file ends
            // with a CRC-64 over its own bytes and a load refuses one that does
            // not match.
            return backup_.load_from(file, store_);
        },
        .apply = [this](const KV::Command &command) {
            return apply_replicated_command(command);
        },
        .link_changed = [](bool up) {
            spdlog::info("replication: {}", up ? "the master is up" : "the master went away");
        },
    };
    // The runtime comes up before the service that borrows it: the service holds a
    // condition variable, which is a channel on the runtime, and asking the engine
    // for one is what brings it up -- with a fallback multiplexer, since nobody
    // has said which one to use yet. Initialize it here, deliberately, and the
    // service is built on the one this server chose.
    const std::string_view multiplexer = options.multiplexer;

    std::unique_ptr<Foundation::NBIO::Multiplexer> mux;
    if (multiplexer == "epoll")
    {
        mux = std::make_unique<Foundation::NBIO::EpollMultiplexer>();
    }
    else if (multiplexer == "io_uring")
    {
        mux = std::make_unique<Foundation::NBIO::URingMultiplexer>();
    }
    else
    {
        throw std::invalid_argument("--multiplexer must be 'epoll' or 'io_uring'");
    }
    switch (mux->type())
    {
    case Foundation::NBIO::MultiplexerType::kEpoll:
        spdlog::info("Multiplexer: epoll");
        break;
    case Foundation::NBIO::MultiplexerType::kURing:
        spdlog::info("Multiplexer: io_uring");
        break;
    }
    Foundation::NBIO::initialize(std::move(mux));

    replication_ = std::make_unique<ReplicationService>(std::move(replication_options), std::move(host));

    if (replication_->is_master())
    {
        Foundation::NBIO::spawn(replication_->serve());
    }
    if (replication_->is_replica())
    {
        spdlog::info("replication: read-only replica of {}:{}", master->ip(), master->port());
        Foundation::NBIO::spawn(replication_->follow());
    }

    Foundation::NBIO::run(start(port, std::move(commands)));
}

Foundation::NBIO::Task<void> Server::start(std::uint16_t port, std::vector<CommandLine> commands)
{
    // The commands go first: a server that is answering clients is a server that
    // has decided how it is configured.
    co_await apply_commands(std::move(commands));

    try
    {
        co_await serve(Foundation::Core::Address::from_ipv4("0.0.0.0", port));
    }
    catch (const std::exception &error)
    {
        // This is fatal, and nothing above this frame can report it: the
        // replication listener, when there is one, parks the runtime waiting for
        // a replica, so the loop never drains and the failure would otherwise
        // leave a process that is alive with no port for clients to reach. Say
        // what happened and end, the way a server that cannot listen should.
        spdlog::error("server stopped: {}", error.what());
        std::exit(EXIT_FAILURE);
    }
}

Foundation::NBIO::Task<void> Server::apply_commands(std::vector<CommandLine> commands)
{
    // Every line is a command, and it is validated and executed by the same code
    // that serves a client: the file configures this server by running it, not
    // through a second set of rules that would drift from the first. A line that
    // cannot be carried out stops the startup, so a mistake is reported now
    // rather than by an instance that is already answering clients.
    for (const CommandLine &line : commands)
    {
        const KV::CommandValidation validation = KV::ValidateCommand(CommandRequest(line));
        if (!validation)
        {
            throw std::runtime_error(line.Where() + ": " + validation.error);
        }

        const RESP::Object response = co_await execute(*validation.command);
        if (const auto *error = std::get_if<RESP::SimpleError>(&response.value))
        {
            throw std::runtime_error(line.Where() + ": " + error->value);
        }
        spdlog::info("config: applied '{}'", line.text);
    }

    if (!commands.empty())
    {
        spdlog::info("config: {} command(s) applied", commands.size());
    }
}

Foundation::NBIO::Task<void> Server::serve(const Foundation::Core::Address &address)
{
    co_await std::move(accept_clients(Foundation::NBIO::bind(address)));
}

Foundation::NBIO::Task<void> Server::accept_clients(std::unique_ptr<Foundation::NBIO::AcceptChannel> acceptor)
{
    while (true)
    {
        auto result = co_await acceptor->accept();
        if (!result)
        {
            continue;
        }
        auto &[socket, address] = *result;
        Foundation::NBIO::spawn(serve_client(std::make_shared<Session>(Foundation::NBIO::establish(std::move(socket)))));
    }
}

namespace
{
// How much of a batch may pile up before it is written. A client that pipelines
// a million commands and never reads the answers must not be able to make the
// server buffer without limit, and a batch past this size is not going to leave
// in one write anyway.
constexpr std::size_t kReplyBatchBytes = 64U * 1024U;
} // namespace

    Foundation::NBIO::Task<void> Server::serve_client(std::shared_ptr<Session> session)
{
    // A pipeline can hold more than one command, and a single command can be
    // larger than a socket read, so the receive buffer starts roomy and is
    // allowed to grow: the decoder needs the whole command before it can hand
    // one over. The send buffer holds the replies of one batch, which is what a
    // pipeline is answered with, so it grows to the size of that batch.
    auto recv_buffer = std::make_unique<::Foundation::Core::Buffer>(64U * 1024U, 16U * 1024U * 1024U);
    auto send_buffer = std::make_unique<::Foundation::Core::Buffer>(16U * 1024U, 16U * 1024U * 1024U);

    RESP::Receiver receiver(session->transport(), *recv_buffer);
    while (true)
    {
        // Waiting for input is the only place this coroutine blocks, and it never
        // does so with replies still in hand: what the client has already sent is
        // answered first.
        auto command = co_await receiver.receive_command();
        if (receiver.no_command())
        {
            // A line with no command on it: nothing is owed to the client for
            // it, and the connection stays up for the command that follows.
            continue;
        }
        if (!command)
        {
            RESP::Sender sender(session->transport(), *send_buffer);
            if (!receiver.decode_error().empty())
            {
                // The bytes that did not parse are already dropped, so the
                // commands behind them are still decodable: the client is told
                // and the connection goes on.
                (void)co_await sender.send(RESP::Object(RESP::SimpleError{
                    .value = receiver.decode_error()
                }));
                continue;
            }
            (void)co_await sender.send(RESP::Object(RESP::SimpleError{
                .value = "internal error"
            }));
            co_return;
        }

        // The batch: this command, and every command already buffered behind it.
        // Replies are appended rather than written, so a pipeline leaves in one
        // write instead of one write per command.
        RESP::Sender sender(session->transport(), *send_buffer);
        while (true)
        {
            // A single-key read is answered out of the words it was read with,
            // before any of this: see answer_read.
            std::optional<RESP::Object> answered =
                command->object ? std::nullopt : answer_read(command->words, *session);
            if (!answered)
            {
                // A command the client wrote as an array of bulk strings is
                // answered out of the words read where they lie; anything else --
                // an inline line, an argument that is not a string -- was decoded
                // into an object and is answered from that.
                const KV::CommandValidation validation = command->object ? KV::ValidateCommand(*command->object)
                                                                        : KV::ValidateCommand(command->words);
                if (validation)
                {
                    answered = co_await dispatch(*session, std::move(*validation.command));
                }
                else
                {
                    answered = detail::Error(validation.error);
                }
            }
            sender.append(*answered);
            if (sender.pending() >= kReplyBatchBytes)
            {
                if (!co_await sender.flush())
                {
                    co_return;
                }
            }

            auto next = receiver.try_receive_command();
            if (next)
            {
                command = std::move(next);
                continue;
            }
            if (!receiver.decode_error().empty())
            {
                sender.append(RESP::Object(RESP::SimpleError{
                    .value = receiver.decode_error()
                }));
            }
            break;
        }

        if (!co_await sender.flush())
        {
            co_return;
        }
    }
}

std::optional<RESP::Object> Server::answer_read(std::span<const std::string_view> words, Session &session)
{
    // A read of one key is most of what a server is asked, and it is worth
    // answering without building a command for it. The answer needs the key the
    // client wrote -- which is already here, in the words the request was read
    // as -- and what the store holds under it. Built as a command, that key
    // becomes a string of its own, which is an allocation for every key longer
    // than the small-string case (a benchmark key is), and the answer then
    // travels out through a validation, a dispatch, a coroutine at every step and
    // a variant, none of which is what looking a key up takes.
    //
    // A transaction is the one case where the command has to be built anyway: a
    // command inside a MULTI is not run when it arrives but when EXEC does, so it
    // has to be kept, with a key that outlives the request it was read from.
    if (session.is_multi || words.size() != 2 || !KV::IsCommandName(words.front(), KV::CommandType::kGet))
    {
        return std::nullopt;
    }

    // The key is copied into the connection's own buffer, which grows to the
    // longest key once and is reused for every lookup after that.
    session.lookup_key.assign(words[1].data(), words[1].size());
    return RESP::Object(RESP::BulkString{.value = store_.get(session.lookup_key)});
}

Foundation::NBIO::Task<RESP::Object> Server::dispatch(Session &session, KV::Command command)
{
    if (command.type == KV::CommandType::kMulti)
    {
        if (session.is_multi)
        {
            co_return detail::Error("ERR MULTI calls can not be nested");
        }
        session.is_multi = true;
        co_return RESP::Object(RESP::SimpleString{.value = "OK"});
    }
    if (command.type == KV::CommandType::kExec)
    {
        if (!session.is_multi)
        {
            co_return detail::Error("ERR EXEC without MULTI");
        }
        session.is_multi = false;
        RESP::Array results;
        results.values.reserve(session.queued_commands.size());
        for (const KV::Command &queued : session.queued_commands)
        {
            results.values.push_back(co_await execute(queued));
        }
        session.queued_commands.clear();
        co_return RESP::Object(std::move(results));
    }
    if (session.is_multi)
    {
        session.queued_commands.push_back(std::move(command));
        co_return RESP::Object(RESP::SimpleString{.value = "QUEUED"});
    }
    co_return co_await execute(command);
}

Foundation::NBIO::Task<RESP::Object> Server::execute(const KV::Command &command)
{
    if (replica_read_only_ && KV::IsWriteCommand(command.type))
    {
        co_return detail::Error("READONLY You can't write against a read only replica.");
    }

    RESP::Object response = detail::Error("ERR command cannot be executed");
    switch (command.type)
    {
    case KV::CommandType::kPing:
        response = co_await execute_ping(command);
        break;
    case KV::CommandType::kInfo:
        response = co_await execute_info(command);
        break;
    case KV::CommandType::kGet:
        response = co_await execute_get(command);
        break;
    {
        (void)command;
        co_return RESP::Object(RESP::SimpleString{.value = "KVStore"});
    }
    case KV::CommandType::kSet:
        response = co_await execute_set(command);
        break;
    case KV::CommandType::kDel:
        response = co_await execute_del(command);
        break;
    case KV::CommandType::kExists:
        response = co_await execute_exists(command);
        break;
    case KV::CommandType::kDbSize:
        response = co_await execute_dbsize(command);
        break;
    case KV::CommandType::kCommand:
        response = co_await execute_command_info(command);
        break;
    case KV::CommandType::kClient:
        response = co_await execute_client(command);
        break;
    case KV::CommandType::kConfig:
        response = co_await execute_config(command);
        break;
    case KV::CommandType::kBgSave:
        response = co_await execute_bgsave(command);
        break;
    case KV::CommandType::kSave:
        response = co_await execute_save(command);
        break;
    default:
        break;
    }

    // The same writes the AOF records are the ones a replica has to be told
    // about, so they go into the replication log too -- and they go in first.
    // The store has already been changed by the time this runs, so the log has
    // to hold the write before this frame awaits: a replica that is handed a
    // snapshot across that await would otherwise be given the write in its image
    // and then read it again from the log. Recording never waits itself.
    if (KV::IsWriteCommand(command.type))
    {
        if (replication_ != nullptr)
        {
            replication_->record(command);
        }
        if (aof_.enabled())
        {
            co_await aof_.append(command);
        }
    }
    co_return response;
}

Foundation::NBIO::Task<RESP::Object> Server::execute_ping(const KV::Command &command)
{
    (void)command;
    co_return RESP::Object(RESP::SimpleString{.value = "PONG"});
}

Foundation::NBIO::Task<RESP::Object> Server::execute_get(const KV::Command &command)
{
    const auto &get = std::get<KV::GetParams>(command.parameters);
    co_return RESP::Object(RESP::BulkString{.value = store_.get(get.key)});
}

Foundation::NBIO::Task<RESP::Object> Server::execute_set(const KV::Command &command)
{
    const auto &set = std::get<KV::SetParams>(command.parameters);
    store_.set(set.key, set.value);
    co_return RESP::Object(RESP::SimpleString{.value = "OK"});
}

Foundation::NBIO::Task<RESP::Object> Server::execute_del(const KV::Command &command)
{
    const auto &del = std::get<KV::DelParams>(command.parameters);
    const bool exists = store_.contains(del.key);
    store_.set(del.key, std::nullopt);
    co_return RESP::Object(RESP::Integer{.value = exists ? 1 : 0});
}

Foundation::NBIO::Task<RESP::Object> Server::execute_exists(const KV::Command &command)
{
    const auto &exists = std::get<KV::ExistsParams>(command.parameters);
    co_return RESP::Object(RESP::Boolean{.value = store_.contains(exists.key)});
}

Foundation::NBIO::Task<RESP::Object> Server::execute_dbsize(const KV::Command &command)
{
    (void)command;
    // The count in the index, like Redis, and not a walk that would also drop the
    // keys whose TTL has passed on the way -- a size query does not get to cost
    // time proportional to the store. A key that has expired but has not been
    // visited yet is therefore still counted, which is also what Redis does.
    co_return RESP::Object(RESP::Integer{.value = static_cast<std::int64_t>(store_.size())});
}

bool Server::replay_aof_command(const KV::Command &command)
{
    switch (command.type)
    {
    case KV::CommandType::kSet: {
        const auto &set = std::get<KV::SetParams>(command.parameters);
        store_.set(set.key, set.value);
        return true;
    }
    case KV::CommandType::kDel: {
        const auto &del = std::get<KV::DelParams>(command.parameters);
        store_.set(del.key, std::nullopt);
        return true;
    }
    case KV::CommandType::kExpire:
        return false;
    default:
        return true;
    }
}

bool Server::apply_replicated_command(const KV::Command &command)
{
    // The same mutations the AOF replay knows, and for the same reason: these are
    // the writes a master applies, and this end is applying the ones its master
    // applied, in the order it applied them. Recording them is what lets a replica
    // of this replica be caught up from the same bytes.
    switch (command.type)
    {
    case KV::CommandType::kSet: {
        const auto &set = std::get<KV::SetParams>(command.parameters);
        store_.set(set.key, set.value);
        break;
    }
    case KV::CommandType::kDel: {
        const auto &del = std::get<KV::DelParams>(command.parameters);
        store_.set(del.key, std::nullopt);
        break;
    }
    default:
        break;
    }

    if (replication_ != nullptr)
    {
        replication_->record(command);
    }
    return true;
}

std::optional<std::string> Server::config_value(const std::string &parameter) const
{
    if (parameter == "appendonly")
    {
        return aof_.enabled() ? "yes" : "no";
    }
    if (parameter == "appendfsync")
    {
        // The AOF is written with pwrite and never synced, which is exactly what
        // Redis calls `appendfsync no`.
        return "no";
    }
    if (parameter == "save")
    {
        // Only the SAVE command writes a snapshot; nothing runs on a schedule.
        return "";
    }
    if (parameter == "port")
    {
        return std::to_string(port_);
    }
    if (parameter == "replication_address")
    {
        // The address and port a replica connects to, or nothing when this server
        // serves no replicas.
        return replication_port_ == 0 ? std::string{}
                                      : replication_address_ + " " + std::to_string(replication_port_);
    }
    return std::nullopt;
}

Foundation::NBIO::Task<RESP::Object> Server::execute_command_info(const KV::Command &command)
{
    (void)command;
    // No command metadata to publish. An empty array is a well-formed answer,
    // which is all redis-cli needs to stop reporting the probe as an error.
    co_return RESP::Object(RESP::Array{});
}

Foundation::NBIO::Task<RESP::Object> Server::execute_info(const KV::Command &command)
{
    (void)command;
    co_return RESP::Object(RESP::SimpleString{.value = "KVStore"});
}

Foundation::NBIO::Task<RESP::Object> Server::execute_client(const KV::Command &command)
{
    const auto &client = std::get<KV::ClientParams>(command.parameters);

    // The handshake subcommands have to succeed. A client announces itself with
    // `CLIENT SETINFO` the moment it connects, and an error there makes it treat
    // the connection as broken before it ever sends a command. Nothing is kept:
    // this server has no per-connection state to name.
    if (client.subcommand == "setinfo" || client.subcommand == "setname" || client.subcommand == "no-evict" ||
        client.subcommand == "no-touch")
    {
        co_return RESP::Object(RESP::SimpleString{.value = "OK"});
    }
    if (client.subcommand == "getname")
    {
        co_return RESP::Object(RESP::BulkString{.value = std::string{}});
    }
    if (client.subcommand == "id")
    {
        co_return RESP::Object(RESP::Integer{.value = 0});
    }

    // CLIENT INFO and CLIENT LIST describe connections this server does not
    // track, so an empty description is the honest answer.
    co_return RESP::Object(RESP::BulkString{.value = std::string{}});
}

Foundation::NBIO::Task<RESP::Object> Server::execute_config(const KV::Command &command)
{
    const auto &config = std::get<KV::ConfigParams>(command.parameters);

    if (config.values.empty())
    {
        // CONFIG GET answers with a flat name/value array, and an unknown name is
        // an empty array rather than an error -- the same as Redis.
        const std::vector<std::string> names =
            config.parameter == "*"
                ? std::vector<std::string>{"appendonly", "appendfsync", "save", "port", "replication_address"}
                : std::vector<std::string>{config.parameter};
        std::vector<RESP::Object> values;
        for (const std::string &name : names)
        {
            if (const auto value = config_value(name))
            {
                values.push_back(RESP::Object(RESP::BulkString{.value = name}));
                values.push_back(RESP::Object(RESP::BulkString{.value = *value}));
            }
        }
        co_return RESP::Object(RESP::Array{.values = std::move(values)});
    }

    if (config.parameter == "appendonly")
    {
        if (config.values.front() == "yes")
        {
            if (!aof_.enable())
            {
                co_return detail::Error("ERR CONFIG SET failed - could not open the AOF");
            }
        }
        else
        {
            aof_.disable();
        }
    }
    else if (KV::IsStartupConfigParameter(config.parameter))
    {
        // The ports are bound and the replication listener is built while the
        // server is being put together, so a running one has nothing left to
        // apply this to: it belongs in a command file, not in a command.
        co_return detail::Error("ERR CONFIG SET failed - '" + config.parameter +
                                "' is a startup setting; it can only be named in the command file");
    }
    co_return RESP::Object(RESP::SimpleString{.value = "OK"});
}

Foundation::NBIO::Task<RESP::Object> Server::execute_bgsave(const KV::Command &command)
{
    (void)command;
    if (!co_await backup_.save(store_))
    {
        co_return detail::Error("ERR failed to save RDB snapshot");
    }
    co_return RESP::Object(RESP::SimpleString{.value = "Background saving started"});
}

Foundation::NBIO::Task<RESP::Object> Server::execute_save(const KV::Command &command)
{
    (void)command;
    // The image is written in this process, so this frame's thread -- the one the
    // event loop runs on -- is busy until the file is in place. That is what SAVE
    // is for: the snapshot on disk before the answer, with the server standing
    // still in the meantime, where BGSAVE hands the writing to a child.
    if (!backup_.save_now(store_))
    {
        co_return detail::Error("ERR failed to save RDB snapshot");
    }
    co_return RESP::Object(RESP::SimpleString{.value = "OK"});
}
} // namespace KV
