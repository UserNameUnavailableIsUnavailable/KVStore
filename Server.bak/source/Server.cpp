#include "Server/Server.hpp"

#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string_view>

#include <sys/wait.h>
#include <unistd.h>

#include "Protocol.hpp"

namespace KV
{
namespace
{
Result MakeResult(ResultType type, std::string_view value)
{
    return {.type = type,
        .value = std::pmr::string(value),
        .elements = std::pmr::vector<Result>()};
}

Result MakeArrayResult()
{
    return {.type = ResultType::kArray,
        .value = std::pmr::string(),
        .elements = std::pmr::vector<Result>()};
}

void AppendResult(Result& result, ResultType type, std::string_view value)
{
    result.elements.push_back({.type = type,
        .value = std::pmr::string(value),
        .elements = std::pmr::vector<Result>()});
}

// Only commands that actually change the data set may be propagated: they are
// the ones an append-only log has to replay and a replica has to apply.
// Read-only lookups (GET, EXISTS, TTL) and administrative commands (SAVE,
// APPendONLY, PSYNC, SLAVEOF) carry no mutation, and replaying them would be
// wrong -- PSYNC in particular would end up inside the very backlog it serves,
// so a replica resuming from an offset would be handed a PSYNC of its own.
// Keep this in sync with Server::RegisterHandlers.
bool IsMutation(std::string_view name)
{
    return name == "SET" || name == "DELETE" || name == "EXPIRE";
}

class ReplicationSession final : public Session
{
public:
    explicit ReplicationSession(NetworkingModel networking_model) :
        Session(networking_model)
    {
    }
};

template <template <typename, typename> class Container>
class IndexedStore final : public Server::Indexer
{
public:
    IndexedStore(std::size_t capacity, std::pmr::memory_resource* resource) :
        store_(capacity, resource)
    {
    }

    std::optional<std::string> Get(const std::string& key) override
    {
        return store_.Get(key);
    }

    void Set(const std::string& key, std::optional<std::string> value) override
    {
        store_.Set(key, std::move(value));
    }

    bool Exists(const std::string& key) override
    {
        return store_.Exists(key);
    }

    bool Expire(const std::string& key, std::chrono::milliseconds ttl) override
    {
        return store_.set_ttl(key, ttl);
    }

    std::optional<std::chrono::milliseconds> TimeToLive(const std::string& key) override
    {
        return store_.get_ttl(key);
    }

    std::vector<SnapshotEntry> Snapshot() override
    {
        std::vector<SnapshotEntry> entries;
        store_.visit_live([&entries](const std::string& key, const std::string& value,
                             std::optional<std::chrono::milliseconds> ttl) {
            entries.push_back({.key = key, .value = value, .ttl = ttl});
        });
        return entries;
    }

private:
    LRUDataStore<std::string, std::string, Container> store_;
};
} // namespace

Server::Server(CacheStrategy cache_strategy, std::string persistence_directory,
    std::pmr::memory_resource* resource) :
    server_socket_(SocketProtocol::kTcp),
    memory_resource_(resource),
    store_(createStore(cache_strategy)),
    cache_strategy_(cache_strategy),
    persistence_directory_(std::move(persistence_directory))
{
	replication_id_ = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count());
	server_socket_.SetReuseAddress();
	RegisterHandlers();
	LoadBackup();
}

void Server::Listen(std::uint16_t port, int backlog)
{
    server_socket_.Bind(port);
    server_socket_.Listen(backlog < 0 ? SOMAXCONN : backlog);
}

std::unique_ptr<Server::Indexer> Server::createStore(CacheStrategy cache_strategy) const
{
    switch (cache_strategy)
    {
    case CacheStrategy::kHash:
        return std::make_unique<IndexedStore<HashMap>>(store_capacity_, memory_resource_);
    case CacheStrategy::kArray:
        return std::make_unique<IndexedStore<ArrayMap>>(store_capacity_, memory_resource_);
    case CacheStrategy::kRedBlackTree:
        return std::make_unique<IndexedStore<RedBlackTreeMap>>(store_capacity_, memory_resource_);
    case CacheStrategy::kSkipList:
        return std::make_unique<IndexedStore<SkipListMap>>(store_capacity_, memory_resource_);
    }
    std::unreachable();
}

Server::~Server() noexcept
{
    ReapSaveProcess(true);
}

Socket::HandleType Server::GetSocketHandle() const noexcept
{
	return server_socket_.get_native_handle();
}

Result Server::Execute(const Command& command, Session* session)
{
    auto it = handlers_.find(std::string(command.name));
    if (it == handlers_.end())
    {
        return MakeResult(ResultType::kError, "unknown command");
    }
    Session* previous_session = executing_session_;
    executing_session_ = session;
    Result result;
    try
    {
        result = it->second(command);
    }
    catch (...)
    {
        executing_session_ = previous_session;
        throw;
    }
    executing_session_ = previous_session;
    if (replaying_ || result.type == ResultType::kError || !IsMutation(command.name))
    {
        return result;
    }
    if (append_only_)
    {
        AppendCommandToBackup(command);
    }
    RecordReplicationCommand(command);
    return result;
}

void Server::RegisterHandlers()
{
    handlers_["GET"] = [this](const Command& command) -> Result
    {
        if (command.arguments.size() != 1)
        {
            return MakeResult(ResultType::kError, "usage: GET key");
        }
        auto value = store_->Get(std::string(command.arguments[0]));
        if (!value.has_value())
        {
            return MakeResult(ResultType::kError, "key not found");
        }
        return MakeResult(ResultType::kBulkString, *value);
    };

    handlers_["SET"] = [this](const Command& command) -> Result
    {
        if (command.arguments.size() != 2)
        {
            return MakeResult(ResultType::kError, "usage: SET key value");
        }
        store_->Set(std::string(command.arguments[0]), std::string(command.arguments[1]));
        return MakeResult(ResultType::kSimpleString, "OK");
    };

    handlers_["DELETE"] = [this](const Command& command) -> Result
    {
        if (command.arguments.size() != 1)
        {
            return MakeResult(ResultType::kError, "usage: DELETE key");
        }
        const std::string key(command.arguments[0]);
        if (!store_->Exists(key))
        {
            return MakeResult(ResultType::kError, "key not found");
        }
        store_->Set(key, std::nullopt);
        return MakeResult(ResultType::kSimpleString, "OK");
    };

    handlers_["EXISTS"] = [this](const Command& command) -> Result
    {
        if (command.arguments.size() != 1)
        {
            return MakeResult(ResultType::kError, "usage: EXISTS key");
        }
        return MakeResult(ResultType::kSimpleString,
            store_->Exists(std::string(command.arguments[0])) ? "YES" : "NO");
    };

    handlers_["EXPIRE"] = [this](const Command& command) -> Result
    {
        if (command.arguments.size() != 2)
        {
            return MakeResult(ResultType::kError, "usage: EXPIRE key ttl-milliseconds");
        }
        std::int64_t milliseconds = 0;
        const std::string_view ttl(command.arguments[1]);
        const auto [end, error] = std::from_chars(ttl.data(), ttl.data() + ttl.size(), milliseconds);
        if (error != std::errc {} || end != ttl.data() + ttl.size() || milliseconds <= 0)
        {
            return MakeResult(ResultType::kError, "ttl must be a positive integer in milliseconds");
        }
        if (!store_->Expire(std::string(command.arguments[0]), std::chrono::milliseconds(milliseconds)))
        {
            return MakeResult(ResultType::kError, "key not found");
        }
        return MakeResult(ResultType::kSimpleString, "OK");
    };

    handlers_["TTL"] = [this](const Command& command) -> Result
    {
        if (command.arguments.size() != 1)
        {
            return MakeResult(ResultType::kError, "usage: TTL key");
        }
        const std::string key(command.arguments[0]);
        if (!store_->Exists(key))
        {
            return MakeResult(ResultType::kError, "key not found");
        }
        const auto ttl = store_->TimeToLive(key);
        return MakeResult(ResultType::kSimpleString,
            ttl.has_value() ? std::to_string(ttl->count()) : "PERSISTENT");
    };

    handlers_["SAVE"] = [this](const Command& command) -> Result
    {
        if (!command.arguments.empty())
        {
            return MakeResult(ResultType::kError, "usage: SAVE");
        }
        return Save() ? MakeResult(ResultType::kSimpleString, "OK") :
                        MakeResult(ResultType::kError, "save failed or already in progress");
    };

    handlers_["APPendONLY"] = [this](const Command& command) -> Result
    {
        if (command.arguments.size() != 1)
        {
            return MakeResult(ResultType::kError, "usage: APPendONLY YES|NO");
        }
        if (command.arguments[0] == "YES")
        {
            std::error_code error;
            std::filesystem::create_directories(std::filesystem::path(persistence_directory_) / "aof", error);
            if (error)
            {
                return MakeResult(ResultType::kError, "could not create AOF directory");
            }
            append_only_ = true;
            append_only_file_ = NewPersistencePath("aof", ".aof");
            return MakeResult(ResultType::kSimpleString, "OK");
        }
        if (command.arguments[0] == "NO")
        {
            append_only_ = false;
            append_only_file_.clear();
            return MakeResult(ResultType::kSimpleString, "OK");
        }
        return MakeResult(ResultType::kError, "APPendONLY must be YES or NO");
    };

    handlers_["PSYNC"] = [this](const Command& command) -> Result
    {
        if (command.arguments.size() != 2)
        {
            return MakeResult(ResultType::kError, "usage: PSYNC replica-id offset");
        }
        std::uint64_t offset = 0;
        const std::string_view text_offset(command.arguments[1]);
        const auto [end, error] = std::from_chars(text_offset.data(), text_offset.data() + text_offset.size(), offset);
        const bool can_continue = std::string_view(command.arguments[0]) == replication_id_ && error == std::errc {} &&
            end == text_offset.data() + text_offset.size() &&
            offset <= replication_offset_ &&
            (replication_backlog_.empty() || offset >= replication_backlog_.front().offset - 1);

        Result response = MakeArrayResult();
        AppendResult(response, ResultType::kSimpleString, can_continue ? "CONTINUE" : "FULLRESYNC");
        AppendResult(response, ResultType::kSimpleString, replication_id_);
        AppendResult(response, ResultType::kSimpleString, std::to_string(replication_offset_));
        const std::pmr::string commands = can_continue
            ? EncodeReplicationCommands(offset)
            : EncodeReplicaSnapshot();
        AppendResult(response, ResultType::kBulkString, commands);
        if (executing_session_ != nullptr)
        {
            RegisterSlave(*executing_session_);
        }
        return response;
    };

    handlers_["SLAVEOF"] = [this](const Command& command) -> Result
    {
        if (command.arguments.size() != 2)
        {
            return MakeResult(ResultType::kError, "usage: SLAVEOF <address> <port>");
        }
        unsigned int port = 0;
        const std::string_view text_port(command.arguments[1]);
        const auto [end, error] = std::from_chars(text_port.data(), text_port.data() + text_port.size(), port);
        if (error != std::errc {} || end != text_port.data() + text_port.size() || port == 0 || port > 65535)
        {
            return MakeResult(ResultType::kError, "port must be an integer between 1 and 65535");
        }
        const std::string_view address(command.arguments[0]);
        if ((address == "127.0.0.1" || address == "0.0.0.0") &&
            GetPort() != 0 && port == GetPort())
        {
            return MakeResult(ResultType::kError, "server cannot replicate from itself");
        }
        return StartReplication(std::string(address), static_cast<std::uint16_t>(port))
            ? MakeResult(ResultType::kSimpleString, "OK")
            : MakeResult(ResultType::kError, "replication sync failed");
    };
}

void Server::LoadBackup()
{
    const auto path = GetLatestPersistencePath();
    if (!path.has_value())
    {
        return;
    }
    std::ifstream input(*path, std::ios::binary);
    if (!input)
    {
        return;
    }
    const std::string encoded((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    Protocol protocol;
    std::vector<Command> commands;
    std::size_t offset = 0;
    while (offset < encoded.size())
    {
        RequestDecode decoded = protocol.DecodeRequest(std::string_view(encoded).substr(offset));
        if (decoded.status != DecodeStatus::kComplete || decoded.consumed_bytes == 0)
        {
            throw std::runtime_error("invalid persistence file");
        }
        offset += decoded.consumed_bytes;
        commands.push_back(std::move(decoded.command));
    }
    ReplayCommands(commands);
}

bool Server::Save()
{
#if defined(_WIN32)
#error "The Save function is currently Linux-specific."
#endif
    ReapSaveProcess(false);
    if (save_process_ != 0)
    {
        return false;
    }

    const pid_t process = ::fork();
    if (process < 0)
    {
        return false;
    }
    if (process == 0)
    {
        ::close(GetSocketHandle());
        _exit(SaveSnapshot() ? 0 : 1);
    }

    save_process_ = process;
    return true;
}

bool Server::SaveSnapshot()
{
    std::error_code error;
    std::filesystem::create_directories(std::filesystem::path(persistence_directory_) / "full", error);
    if (error)
    {
        return false;
    }
    std::ofstream output(NewPersistencePath("full", ".ful"), std::ios::binary);
    if (!output)
    {
        return false;
    }
    Protocol protocol;
    const auto write = [&output, &protocol](std::string_view name, std::initializer_list<std::string> arguments) {
        Command command {.name = std::pmr::string(name), .arguments = std::pmr::vector<std::pmr::string>()};
        for (const std::string& argument : arguments)
        {
            command.arguments.emplace_back(argument);
        }
        const std::pmr::string encoded = protocol.EncodeRequest(command);
        output.write(encoded.data(), static_cast<std::streamsize>(encoded.size()));
    };
    write("MULTI", {});
    for (const SnapshotEntry& entry : store_->Snapshot())
    {
        write("SET", {entry.key, entry.value});
        if (entry.ttl.has_value())
        {
            write("EXPIRE", {entry.key, std::to_string(entry.ttl->count())});
        }
    }
    write("EXEC", {});
    return static_cast<bool>(output);
}

void Server::ReapSaveProcess(bool wait) noexcept
{
    if (save_process_ == 0)
    {
        return;
    }

    const pid_t process = static_cast<pid_t>(save_process_);
    pid_t result = 0;
    do
    {
        result = ::waitpid(process, nullptr, wait ? 0 : WNOHANG);
    } while (result == -1 && errno == EINTR);

    if (result == process || (result == -1 && errno == ECHILD))
    {
        save_process_ = 0;
    }
}

void Server::AppendCommandToBackup(const Command& command)
{
    if (append_only_file_.empty())
    {
        append_only_file_ = NewPersistencePath("aof", ".aof");
    }
    std::ofstream output(append_only_file_, std::ios::app | std::ios::binary);
    if (!output)
    {
        return;
    }
    Protocol protocol;
    const std::pmr::string encoded = protocol.EncodeRequest(command);
    output.write(encoded.data(), static_cast<std::streamsize>(encoded.size()));
}

void Server::RecordReplicationCommand(const Command& command)
{
    ++replication_offset_;
    replication_backlog_.push_back({
        .offset = replication_offset_,
        .command = command
    });
    if (replication_backlog_.size() > replication_backlog_capacity_)
    {
        replication_backlog_.pop_front();
    }

    // A successful PSYNC turns the duplicated connection into a persistent
    // replication stream. Push each subsequent mutation on that same socket;
    // PSYNC itself never reaches this function because it is not a mutation.
    Protocol protocol;
    const std::pmr::string encoded = protocol.EncodeRequest(command);
    for (auto it = slaves_.begin(); it != slaves_.end();)
    {
        const int handle = (*it)->GetSocket().get_native_handle();
        std::size_t sent = 0;
        bool delivered = handle >= 0;
        while (delivered && sent < encoded.size())
        {
            const ssize_t result = ::send(handle, encoded.data() + sent,
                encoded.size() - sent, MSG_NOSIGNAL | MSG_DONTwait);
            if (result > 0)
            {
                sent += static_cast<std::size_t>(result);
                continue;
            }
            // A slow or closed replica is removed. There is deliberately no
            // blocking retry here: replication must not stall the master loop.
            delivered = false;
        }
        if (!delivered)
        {
            it = slaves_.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void Server::RegisterSlave(const Session& session)
{
    const int connection = session.GetSocket().get_native_handle();
    if (connection < 0)
    {
        return;
    }
    const int duplicate = ::dup(connection);
    if (duplicate >= 0)
    {
        auto slave = std::make_unique<ReplicationSession>(session.GetNetworkingModel());
        slave->AttachSocket(duplicate);
        slaves_.push_back(std::move(slave));
    }
}

std::pmr::string Server::EncodeReplicaSnapshot() const
{
    Protocol protocol;
    std::pmr::string encoded;
    const auto append = [&encoded, &protocol](std::string_view name, std::initializer_list<std::string> arguments) {
        Command command {.name = std::pmr::string(name), .arguments = std::pmr::vector<std::pmr::string>()};
        for (const std::string& argument : arguments)
        {
            command.arguments.emplace_back(argument);
        }
        encoded += protocol.EncodeRequest(command);
    };
    append("MULTI", {});
    for (const SnapshotEntry& entry : store_->Snapshot())
    {
        append("SET", {entry.key, entry.value});
        if (entry.ttl.has_value())
        {
            append("EXPIRE", {entry.key, std::to_string(entry.ttl->count())});
        }
    }
    append("EXEC", {});
    return encoded;
}

std::pmr::string Server::EncodeReplicationCommands(std::uint64_t offset) const
{
    Protocol protocol;
    std::pmr::string encoded;
    for (const ReplicationEntry& entry : replication_backlog_)
    {
        if (entry.offset > offset)
        {
            encoded += protocol.EncodeRequest(entry.command);
        }
    }
    return encoded;
}

bool Server::SynchronizeFromMaster(const std::string& address, std::uint16_t port)
{
    Socket socket(SocketProtocol::kTcp);
    socket.Connect(address, port);
    const NetworkingModel networking_model = executing_session_ != nullptr
        ? executing_session_->GetNetworkingModel()
        : NetworkingModel::kReactor;
    master_session_ = std::make_unique<ReplicationSession>(networking_model);
    master_session_->AttachSocket(std::move(socket));
    Protocol protocol;
    Command request {
        .name = std::pmr::string("PSYNC"),
        .arguments = std::pmr::vector<std::pmr::string>()
    };
    request.arguments.emplace_back(master_replication_id_);
    request.arguments.emplace_back(std::to_string(master_replication_offset_));
    const std::pmr::string encoded = protocol.EncodeRequest(request);
    std::size_t sent = 0;
    while (sent < encoded.size())
    {
        const std::ptrdiff_t written = ::send(master_session_->GetSocket().get_native_handle(), encoded.data() + sent,
            encoded.size() - sent, MSG_NOSIGNAL);
        if (written <= 0)
        {
            master_session_.reset();
            return false;
        }
        sent += static_cast<std::size_t>(written);
    }

    std::string response;
    std::array<char, 4096> buffer;
    while (true)
    {
        const std::ptrdiff_t received = ::recv(master_session_->GetSocket().get_native_handle(), buffer.data(), buffer.size(), 0);
        if (received <= 0)
        {
            master_session_.reset();
            return false;
        }
        response.append(buffer.data(), static_cast<std::size_t>(received));
        const ResponseDecode decoded = protocol.DecodeResponse(response);
        if (decoded.status == DecodeStatus::kIncomplete)
        {
            continue;
        }
        if (decoded.status != DecodeStatus::kComplete || decoded.result.type != ResultType::kArray ||
            decoded.result.elements.size() != 4 || decoded.result.elements[0].type != ResultType::kSimpleString ||
            decoded.result.elements[3].type != ResultType::kBulkString)
        {
            master_session_.reset();
            return false;
        }
        const bool full_sync = decoded.result.elements[0].value == "FULLRESYNC";
        if (!full_sync && decoded.result.elements[0].value != "CONTINUE")
        {
            master_session_.reset();
            return false;
        }
        std::uint64_t offset = 0;
        const std::string_view text_offset(decoded.result.elements[2].value);
        const auto [end, error] = std::from_chars(text_offset.data(), text_offset.data() + text_offset.size(), offset);
        if (error != std::errc {} || end != text_offset.data() + text_offset.size())
        {
            master_session_.reset();
            return false;
        }

        std::vector<Command> commands;
        const std::string_view stream(decoded.result.elements[3].value);
        std::size_t command_offset = 0;
        while (command_offset < stream.size())
        {
            RequestDecode command = protocol.DecodeRequest(stream.substr(command_offset));
            if (command.status != DecodeStatus::kComplete || command.consumed_bytes == 0)
            {
                master_session_.reset();
                return false;
            }
            command_offset += command.consumed_bytes;
            commands.push_back(std::move(command.command));
        }
        if (full_sync)
        {
            store_ = createStore(cache_strategy_);
        }
        ReplayCommands(commands);
        master_replication_id_ = std::string(decoded.result.elements[1].value);
        master_replication_offset_ = offset;
        return true;
    }
}

std::string Server::NewPersistencePath(std::string_view category, std::string_view extension) const
{
    const auto timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return (std::filesystem::path(persistence_directory_) / category /
        (std::to_string(timestamp) + std::string(extension))).string();
}

std::optional<std::string> Server::GetLatestPersistencePath() const
{
    std::optional<std::filesystem::path> latest;
    for (const auto [category, extension] : {std::pair {"aof", ".aof"}, std::pair {"full", ".ful"}})
    {
        const std::filesystem::path directory = std::filesystem::path(persistence_directory_) / category;
        std::error_code error;
        for (const std::filesystem::directory_entry& entry :
            std::filesystem::directory_iterator(directory, error))
        {
            if (error || !entry.is_regular_file() || entry.path().extension() != extension)
            {
                continue;
            }
            if (!latest.has_value() || entry.path().stem() > latest->stem())
            {
                latest = entry.path();
            }
        }
    }
    return latest.has_value() ? std::optional {latest->string()} : std::nullopt;
}

void Server::ReplayCommands(const std::vector<Command>& commands)
{
    replaying_ = true;
    bool in_transaction = false;
    std::vector<Command> transaction;
    for (const Command& command : commands)
    {
        if (command.name == "MULTI")
        {
            in_transaction = true;
            transaction.clear();
        }
        else if (command.name == "EXEC" && in_transaction)
        {
            for (const Command& queued : transaction)
            {
                Execute(queued);
            }
            transaction.clear();
            in_transaction = false;
        }
        else if (in_transaction)
        {
            transaction.push_back(command);
        }
        else
        {
            Execute(command);
        }
    }
    replaying_ = false;
}

} // namespace KV
