#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <memory_resource>
#include <optional>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

#include "Common/Socket.hpp"
#include "Common/Command.hpp"
#include "Common/DataStore.hpp"
#include "Common/Result.hpp"
#include "Common/Session.hpp"

namespace KV
{
enum class CacheStrategy
{
    kHash,
    kArray,
    kRedBlackTree,
    kSkipList,
};

struct SnapshotEntry
{
    std::string key;
    std::string value;
    std::optional<std::chrono::milliseconds> ttl;
};

class Server
{
public:
    explicit Server(CacheStrategy cache_strategy = CacheStrategy::kHash,
        std::string persistence_directory = "persistent",
        std::pmr::memory_resource* resource = std::pmr::get_default_resource());
    void Listen(std::uint16_t port, int backlog = -1);
    std::uint16_t GetPort()
    {
        if (!server_socket_.IsOpen())
        {
            return 0;
        }
        Address local;
        server_socket_.GetLocalAddress(local);
        return local.GetPort();
    }

    virtual void Run(std::uint16_t port, int backlog = -1) = 0;
    virtual ~Server() noexcept;

    class Indexer
    {
    public:
        virtual ~Indexer() = default;
        virtual std::optional<std::string> Get(const std::string& key) = 0;
        virtual void Set(const std::string& key, std::optional<std::string> value) = 0;
        virtual bool Exists(const std::string& key) = 0;
        virtual bool Expire(const std::string& key, std::chrono::milliseconds ttl) = 0;
        virtual std::optional<std::chrono::milliseconds> TimeToLive(const std::string& key) = 0;
        virtual std::vector<SnapshotEntry> Snapshot() = 0;
    };

protected:
    // Start a long-lived replica session. Concrete networking models provide
    // the coroutine and event-queue implementation; the base command handler
    // only owns the replication policy and validation.
    virtual bool StartReplication(const std::string& address, std::uint16_t port)
    {
        (void)address;
        (void)port;
        return false;
    }

    Result Execute(const Command& command, Session* session = nullptr);
    Socket::HandleType GetSocketHandle() const noexcept;
    std::pmr::memory_resource* GetMemoryResource() const
    {
        return memory_resource_;
    }

    using CommandHandler = std::function<Result(const Command&)>;
    struct ReplicationEntry
    {
        std::uint64_t offset = 0;
        Command command;
    };

    std::unique_ptr<Indexer> CreateStore(CacheStrategy cache_strategy) const;
    void RegisterHandlers();
    void LoadBackup();
    bool Save();
    bool SaveSnapshot();
    void ReapSaveProcess(bool wait) noexcept;
    void AppendCommandToBackup(const Command& command);
    // ReplayCommands replays commands from a backup to restore data.
    void ReplayCommands(const std::vector<Command>& commands);
    void RecordReplicationCommand(const Command& command);
    void RegisterSlave(const Session& session);
    // EncodeReplicaSnapshot encodes the entire DB for full data sync for the replica.
    std::pmr::string EncodeReplicaSnapshot() const;
    // EncodeReplicationCommand encodes recently executed commands for replica, allowing incremental sync.
    std::pmr::string EncodeReplicationCommands(std::uint64_t offset) const;
    bool SynchronizeFromMaster(const std::string& address, std::uint16_t port);
    std::string NewPersistencePath(std::string_view category, std::string_view extension) const;
    std::optional<std::string> GetLatestPersistencePath() const;
    static constexpr std::size_t store_capacity_ = 32;

    Socket server_socket_;
    std::unordered_map<std::string, CommandHandler> handlers_;
    Session* executing_session_ = nullptr;
    std::pmr::memory_resource* memory_resource_ = std::pmr::get_default_resource();
    std::unique_ptr<Indexer> store_;
    CacheStrategy cache_strategy_;
    std::string persistence_directory_;
    std::string append_only_file_;
    bool append_only_ = false;
    bool replaying_ = false;
    std::int64_t save_process_ = 0;
    std::string replication_id_;
    std::uint64_t replication_offset_ = 0;
    std::deque<ReplicationEntry> replication_backlog_;
    std::list<std::unique_ptr<Session>> slaves_;
    std::unique_ptr<Session> master_session_;
    std::string master_replication_id_ = "?";
    std::uint64_t master_replication_offset_ = 0;
    static constexpr std::size_t replication_backlog_capacity_ = 256;
};
} // namespace KV
