#include "KVServer.hpp"

#include <charconv>
#include <string>
#include <string_view>
#include <utility>

#include <Foundation/Async/Async.hpp>
#include "Address.hpp"
#include <Foundation/Socket.hpp>

#include "::Foundation::Buffer.hpp"
#include "DataStore.hpp"

namespace KV
{
namespace
{
// Adapts a DataStore policy with a chosen index container to the Indexer API.
template <typename Store> class StoreIndexer final : public Indexer
{
  public:
        explicit StoreIndexer(std::size_t capacity) : store_(capacity)
    {
    }

    std::optional<std::string> get(const std::string &key) override
    {
        return store_.get(key);
    }
    void set(const std::string &key, std::optional<std::string> value) override
    {
        store_.set(key, std::move(value));
    }
    bool exists(const std::string &key) override
    {
        return store_.contains(key);
    }
    bool expire(const std::string &key, std::chrono::milliseconds ttl) override
    {
        return store_.set_ttl(key, ttl);
    }
    std::optional<std::chrono::milliseconds> ttl(const std::string &key) override
    {
        return store_.get_ttl(key);
    }
    void for_each(const std::function<void(std::string_view, std::string_view)> &fn) override
    {
        store_.visit_live([&fn](const std::string &key, const std::string &value,
                                std::optional<std::chrono::milliseconds>) { fn(key, value); });
    }

  private:
    Store store_;
};

template <template <typename, typename, template <typename, typename> class> class Policy>
std::unique_ptr<Indexer> MakeStore(CacheStrategy strategy, std::size_t capacity)
{
    switch (strategy)
    {
    case CacheStrategy::kArray:
        return std::make_unique<StoreIndexer<Policy<std::string, std::string, ArrayMap>>>(capacity);
    case CacheStrategy::kRedBlackTree:
        return std::make_unique<StoreIndexer<Policy<std::string, std::string, RedBlackTreeMap>>>(capacity);
    case CacheStrategy::kSkipList:
        return std::make_unique<StoreIndexer<Policy<std::string, std::string, SkipListMap>>>(capacity);
    case CacheStrategy::kHash:
    default:
        return std::make_unique<StoreIndexer<Policy<std::string, std::string, HashMap>>>(capacity);
    }
}

// Build a SET command for a (key,value) pair (used for snapshot / full sync).
Request MakeSetCommand(std::string_view key, std::string_view value)
{
    Request command;
    command.name = "SET";
    command.arguments.emplace_back(key);
    command.arguments.emplace_back(value);
    return command;
}
} // namespace

KVServer::KVServer(CacheStrategy strategy, std::size_t capacity, std::string persistence_directory,
                   EvictionPolicy eviction_policy)
    : store_(eviction_policy == EvictionPolicy::kLFU ? MakeStore<LFUDataStore>(strategy, capacity)
                                                     : MakeStore<LRUDataStore>(strategy, capacity)),
      persistence_(std::move(persistence_directory))
{
    register_handlers();
}

Result KVServer::make_result(ResultType type, std::string_view value) const
{
    return Result{.type = type, .value = std::pmr::string(value), .elements = {}};
}

bool KVServer::is_mutation(std::string_view name) noexcept
{
    return name == "SET" || name == "DELETE" || name == "EXPIRE";
}

// -------- command execution ------------------------------------------------

Result KVServer::dispatch(const Request &command)
{
    const auto it = handlers_.find(std::string(command.name));
    if (it == handlers_.end())
    {
        return make_result(ResultType::kError, "unknown command");
    }
    return it->second(command);
}

Result KVServer::execute(const Request &command)
{
    Result result = dispatch(command);
    if (replaying_ || result.type == ResultType::kError || !is_mutation(command.name))
    {
        return result;
    }
    // Persist + replicate every successful mutation.
    if (persistence_.is_aof_enabled())
    {
        const std::pmr::string encoded = protocol_.encode_request(command);
        persistence_.append_command(std::string_view(encoded.data(), encoded.size()));
    }
    forward(command);
    return result;
}

void KVServer::forward(const Request &command)
{
    if (replicas_.empty())
    {
        return;
    }
    const std::pmr::string encoded = protocol_.encode_request(command);
    std::string bytes(encoded.data(), encoded.size());
    for (auto &feed : replicas_)
    {
        feed->Push(bytes);
    }
}

// -------- handlers ---------------------------------------------------------

void KVServer::register_handlers()
{
    handlers_["PING"] = [this](const Request &command) -> Result {
        if (!command.arguments.empty())
        {
            return make_result(ResultType::kBulkString, std::string_view(command.arguments[0]));
        }
        return make_result(ResultType::kSimpleString, "PONG");
    };

    handlers_["GET"] = [this](const Request &command) -> Result {
        if (command.arguments.size() != 1)
        {
            return make_result(ResultType::kError, "usage: GET key");
        }
        auto value = store_->get(std::string(command.arguments[0]));
        if (!value.has_value())
        {
            return make_result(ResultType::kError, "key not found");
        }
        return make_result(ResultType::kBulkString, *value);
    };

    handlers_["SET"] = [this](const Request &command) -> Result {
        if (command.arguments.size() != 2)
        {
            return make_result(ResultType::kError, "usage: SET key value");
        }
        store_->set(std::string(command.arguments[0]), std::string(command.arguments[1]));
        return make_result(ResultType::kSimpleString, "OK");
    };

    handlers_["DELETE"] = [this](const Request &command) -> Result {
        if (command.arguments.size() != 1)
        {
            return make_result(ResultType::kError, "usage: DELETE key");
        }
        const std::string key(command.arguments[0]);
        if (!store_->exists(key))
        {
            return make_result(ResultType::kError, "key not found");
        }
        store_->set(key, std::nullopt);
        return make_result(ResultType::kSimpleString, "OK");
    };

    handlers_["EXISTS"] = [this](const Request &command) -> Result {
        if (command.arguments.size() != 1)
        {
            return make_result(ResultType::kError, "usage: EXISTS key");
        }
        return make_result(ResultType::kSimpleString, store_->exists(std::string(command.arguments[0])) ? "YES" : "NO");
    };

    handlers_["EXPIRE"] = [this](const Request &command) -> Result {
        if (command.arguments.size() != 2)
        {
            return make_result(ResultType::kError, "usage: EXPIRE key ttl-milliseconds");
        }
        std::int64_t milliseconds = 0;
        const std::string_view ttl(command.arguments[1]);
        const auto [end, error] = std::from_chars(ttl.data(), ttl.data() + ttl.size(), milliseconds);
        if (error != std::errc{} || end != ttl.data() + ttl.size() || milliseconds <= 0)
        {
            return make_result(ResultType::kError, "ttl must be a positive integer in milliseconds");
        }
        if (!store_->expire(std::string(command.arguments[0]), std::chrono::milliseconds(milliseconds)))
        {
            return make_result(ResultType::kError, "key not found");
        }
        return make_result(ResultType::kSimpleString, "OK");
    };

    handlers_["TTL"] = [this](const Request &command) -> Result {
        if (command.arguments.size() != 1)
        {
            return make_result(ResultType::kError, "usage: TTL key");
        }
        const std::string key(command.arguments[0]);
        if (!store_->exists(key))
        {
            return make_result(ResultType::kError, "key not found");
        }
        const auto ttl = store_->ttl(key);
        return make_result(ResultType::kSimpleString, ttl.has_value() ? std::to_string(ttl->count()) : "PERSISTENT");
    };

    handlers_["SAVE"] = [this](const Request &command) -> Result {
        if (!command.arguments.empty())
        {
            return make_result(ResultType::kError, "usage: SAVE");
        }
        const bool started =
            persistence_.save_snapshot([this](const Persistence::KeyValueSink &sink) { store_->for_each(sink); });
        return started ? make_result(ResultType::kSimpleString, "BGSAVE started")
                       : make_result(ResultType::kError, "save already in progress or fork failed");
    };

    handlers_["APPendONLY"] = [this](const Request &command) -> Result {
        if (command.arguments.size() != 1)
        {
            return make_result(ResultType::kError, "usage: APPendONLY YES|NO");
        }
        if (command.arguments[0] == "YES")
        {
            return persistence_.enable_aof() ? make_result(ResultType::kSimpleString, "OK")
                                            : make_result(ResultType::kError, "could not open AOF");
        }
        if (command.arguments[0] == "NO")
        {
            persistence_.disable_aof();
            return make_result(ResultType::kSimpleString, "OK");
        }
        return make_result(ResultType::kError, "APPendONLY must be YES or NO");
    };

    handlers_["SLAVEOF"] = [this](const Request &command) -> Result {
        if (command.arguments.size() != 2)
        {
            return make_result(ResultType::kError, "usage: SLAVEOF host port");
        }
        unsigned int port = 0;
        const std::string_view text(command.arguments[1]);
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), port);
        if (error != std::errc{} || end != text.data() + text.size() || port == 0 || port > 65535)
        {
            return make_result(ResultType::kError, "port must be an integer between 1 and 65535");
        }
        // Spawn the replication client; it runs concurrently on this engine.
        master_host_ = std::string(command.arguments[0]);
        master_port_ = static_cast<std::uint16_t>(port);
        Async::spawn(replicate_from(master_host_, master_port_));
        return make_result(ResultType::kSimpleString, "OK");
    };
    // SYNC is handled in HandleConnection (it needs the session).
}

// -------- replication ------------------------------------------------------

void KVServer::begin_replication(std::shared_ptr<Async::Session> session)
{
    auto &scheduler = Async::detail::Engine::instance().scheduler();
    auto feed = std::make_shared<ReplicationFeed>(session, scheduler);

    // Seed the full dataset as SET commands, THEN register for forwarding --
    // no co_await in between, so (single-threaded) snapshot+stream is gap-free.
    store_->for_each([&](std::string_view key, std::string_view value) {
        const std::pmr::string encoded = protocol_.encode_request(MakeSetCommand(key, value));
        feed->Push(std::string(encoded.data(), encoded.size()));
    });
    replicas_.push_back(feed);

    // Spawn the sole writer for this slave; it owns the session from now on.
    Async::spawn(feed->run([this](ReplicationFeed *dead) {
        replicas_.remove_if([dead](const std::shared_ptr<ReplicationFeed> &f) { return f.get() == dead; });
    }));
}

// -------- connection loop --------------------------------------------------

Async::Task<bool> KVServer::respond(std::shared_ptr<Async::Session> &session, const Result &result)
{
    const std::pmr::string encoded = protocol_.encode_response(result);
    auto out = std::make_unique<::Foundation::Buffer>(encoded.empty() ? 1 : encoded.size());
    out->append(encoded.data(), encoded.size());
    auto [buffer, send_result] = co_await session->send(std::move(out));
    (void)buffer;
    co_return send_result.status == ::SendStatus::kDone;
}

Async::Task<void> KVServer::connection_handler(std::shared_ptr<Async::Session> session)
{
    auto buffer = std::make_unique<::Foundation::Buffer>(4096);

    while (true)
    {
        RequestDecode decoded = protocol_.decode_request(buffer->string_view());

        if (decoded.status == DecodeStatus::kIncomplete)
        {
            auto [buf, result] = co_await session->receive(std::move(buffer));
            buffer = std::move(buf);
            if (result.status != ::ReceiveStatus::kDone)
            {
                co_return;
            }
            continue;
        }

        if (decoded.status == DecodeStatus::kProtocolError)
        {
            co_await respond(session, make_result(ResultType::kError, std::string_view(decoded.error)));
            co_return;
        }

        buffer->consume(decoded.consumed_bytes);

        // SYNC: hand this connection over to replication and stop the normal
        // request loop. The feed's writer coroutine keeps the session alive.
        if (decoded.command.name == "SYNC")
        {
            begin_replication(std::move(session));
            co_return;
        }

        const Result result = execute(decoded.command);
        if (!co_await respond(session, result))
        {
            co_return;
        }
    }
}

Async::Task<void> KVServer::acceptor(std::uint16_t port)
{
    auto listener = Async::Net::listen(Address::from_ipv4("0.0.0.0", port));

    // If configured as a replica, start the master replication client.
    if (master_port_ != 0)
    {
        Async::spawn(replicate_from(master_host_, master_port_));
    }

    while (true)
    {
        auto [socket, result] = co_await listener->accept();
        if (result.status != ::AcceptStatus::kDone)
        {
            continue;
        }
        auto session = Async::Net::establish(std::move(socket));
        // Fire-and-forget: the scheduler owns this root; cancelAll() reaches it
        // at shutdown, so we do not track the token here.
        Async::spawn(connection_handler(std::move(session)));

        // Opportunistically reap a finished background SAVE child.
        persistence_.reap_save(false);
    }
}

Async::Task<void> KVServer::shutdown_watcher()
{
    co_await Async::wait_for_signal();
    // Let any background SAVE finish so its snapshot is not truncated.
    persistence_.reap_save(true);
    // cancel every root the scheduler owns -- this accept loop (via Serve), all
    // connections, all replica feeds, and this watcher itself. Each teardown
    // detaches parked I/O by RAII, so Engine::run drains to a clean stop.
    Async::detail::Engine::instance().scheduler().cancel_all();
    co_return;
}

Async::Task<void> KVServer::run(std::uint16_t port)
{
    // A separate root watches for the shutdown signal; it will cancelAll(),
    // which cancels this coroutine (and its awaited AcceptLoop) too.
    co_await acceptor(port);
}

// -------- startup restore --------------------------------------------------

void KVServer::load_snapshot()
{
    replaying_ = true;
    // Snapshot first (bulk KV load, no replay), then AOF on top (mutations
    // since the snapshot).
    persistence_.load_snapshot(
        [this](std::string key, std::string value) { store_->set(key, std::optional<std::string>(std::move(value))); });
    persistence_.replay_aof([this](const Request &command) { dispatch(command); });
    replaying_ = false;
}

// -------- replication: slave side ------------------------------------------

void KVServer::attach_master(std::string host, std::uint16_t port)
{
    master_host_ = std::move(host);
    master_port_ = port;
}

Async::Task<void> KVServer::replicate_from(std::string host, std::uint16_t port)
{
    // Connect to the master. A blocking connect is fine here: it is one-time
    // and, for a healthy master, returns immediately (localhost). If the master
    // is not up yet, retry a few times with a backoff.
    // TODO: an async ConnectChannel would avoid blocking the loop during a slow
    // remote handshake.
    Socket socket;
    bool connected = false;
    for (int attempt = 0; attempt < 20 && !connected; ++attempt)
    {
        bool failed = false;
        try
        {
            const Address address = Address::from_ipv4(host, port);
            Socket fresh(address.family(), Socket::Type::kStream);
            fresh.connect(address);
            socket = std::move(fresh);
            connected = true;
        }
        catch (const std::exception &)
        {
            failed = true; // co_await cannot appear inside a catch block
        }
        if (failed)
        {
            co_await Async::sleep_for(std::chrono::milliseconds(200));
        }
    }
    if (!connected)
    {
        co_return; // gave up: master unreachable
    }

    auto session = Async::Net::establish(std::move(socket));

    // Send SYNC to request a full sync followed by the live command stream.
    {
        Request sync;
        sync.name = "SYNC";
        const std::pmr::string encoded = protocol_.encode_request(sync);
        auto out = std::make_unique<::Foundation::Buffer>(encoded.empty() ? 1 : encoded.size());
        out->append(encoded.data(), encoded.size());
        auto [buf, result] = co_await session->send(std::move(out));
        (void)buf;
        if (result.status != ::SendStatus::kDone)
        {
            co_return;
        }
    }

    // Apply the streamed commands. The master sends RESP request frames (SET
    // ...), so decode them exactly like a client request and apply via Dispatch
    // (no re-persist / re-forward -- that is Execute's job).
    auto buffer = std::make_unique<::Foundation::Buffer>(4096);
    while (true)
    {
        RequestDecode decoded = protocol_.decode_request(buffer->string_view());

        if (decoded.status == DecodeStatus::kIncomplete)
        {
            auto [buf, result] = co_await session->receive(std::move(buffer));
            buffer = std::move(buf);
            if (result.status != ::ReceiveStatus::kDone)
            {
                co_return; // master closed the link
            }
            continue;
        }
        if (decoded.status == DecodeStatus::kProtocolError)
        {
            co_return;
        }
        buffer->consume(decoded.consumed_bytes);
        dispatch(decoded.command);
    }
}
} // namespace KV
