#pragma once

// KVServer: the KV store re-hosted on the Async coroutine framework, with
// persistence (SAVE snapshot + AOF) and master->slave replication.
//
//   * Serve()            -- the accept loop.
//   * HandleConnection() -- one coroutine per connection: Receive -> decode
//                           RESP -> Execute -> Send. Intercepts SYNC to turn a
//                           connection into a replica feed.
//   * Execute()          -- Dispatch + persist mutations to the AOF + forward
//                           them to every registered slave.
//
// The server layer no longer threads a pmr memory_resource: the reused
// vocabulary types are pmr but use the default resource.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Foundation/Async/Session.hpp>
#include <Foundation/Async/Task.hpp>

#include "Command.hpp"
#include "Protocol.hpp"
#include "Result.hpp"

#include "Persistence.hpp"
#include "Replication.hpp"

namespace KV
{
enum class CacheStrategy
{
    kHash,
    kArray,
    kRedBlackTree,
    kSkipList,
};

enum class EvictionPolicy
{
  kLRU,
  kLFU,
};

// runtime-selectable index over the DataStore variants.
class Indexer
{
  public:
    virtual ~Indexer() = default;
    virtual std::optional<std::string> get(const std::string &key) = 0;
    virtual void set(const std::string &key, std::optional<std::string> value) = 0;
    virtual bool exists(const std::string &key) = 0;
    virtual bool expire(const std::string &key, std::chrono::milliseconds ttl) = 0;
    virtual std::optional<std::chrono::milliseconds> ttl(const std::string &key) = 0;
    // Enumerate live (key,value) pairs -- used for snapshots and full sync.
    virtual void for_each(const std::function<void(std::string_view, std::string_view)> &fn) = 0;
};

class KVServer
{
  public:
    explicit KVServer(CacheStrategy strategy = CacheStrategy::kHash, std::size_t capacity = 0,
              std::string persistence_directory = "persistent",
              EvictionPolicy eviction_policy = EvictionPolicy::kLRU);

    // Restore from snapshot then AOF. Call before Serve().
    void load_snapshot();

    // Become a replica of the given master (connect + SYNC + apply stream).
    // Effective from the next Serve(): the accept loop spawns the replication
    // coroutine. May also be triggered at runtime via the SLAVEOF command.
    void attach_master(std::string host, std::uint16_t port);

    // Root coroutine: run with Async::detail::Engine::run(server.Serve(port)).
    // Serve spawns a shutdown watcher and then runs the accept loop. When
    // SIGINT/SIGTERM arrives the watcher calls Scheduler::cancelAll(), which
    // tears down the accept loop and every connection/replica, so Engine::run
    // returns cleanly.
    Async::Task<void> run(std::uint16_t port);

  private:
    // The infinite accept loop (one spawned coroutine per connection).
    Async::Task<void> acceptor(std::uint16_t port);
    // waits for a shutdown signal, then cancels every coroutine on the engine.
    Async::Task<void> shutdown_watcher();

    Async::Task<void> connection_handler(std::shared_ptr<Async::Session> session);
    Async::Task<bool> respond(std::shared_ptr<Async::Session> &session, const Result &result);

    // Slave side: connect to master, send SYNC, then decode and apply the
    // streamed command feed (full snapshot followed by live mutations).
    Async::Task<void> replicate_from(std::string host, std::uint16_t port);

    // run a command through the handler table, then persist/replicate mutations.
    Result execute(const Request &command);
    Result dispatch(const Request &command);

    // Replication: seed a new slave with the dataset, then stream mutations.
    void begin_replication(std::shared_ptr<Async::Session> session);
    void forward(const Request &command);

    void register_handlers();
    Result make_result(ResultType type, std::string_view value) const;
    static bool is_mutation(std::string_view name) noexcept;

    Protocol protocol_;
    std::unique_ptr<Indexer> store_;
    std::unordered_map<std::string, std::function<Result(const Request &)>> handlers_;

    Persistence persistence_;
    bool replaying_ = false; // true while restoring: do not re-persist/forward

    // Live slave feeds. Each is written by its own spawned writer coroutine.
    std::list<std::shared_ptr<ReplicationFeed>> replicas_;

    // Master to replicate from (slave side); master_port_ == 0 means "none".
    std::string master_host_;
    std::uint16_t master_port_ = 0;
};
} // namespace KV
