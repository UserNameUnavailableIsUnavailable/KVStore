#pragma once

#include <memory>
#include <Foundation/NBIO/Runtime.hpp>
#include <string>
#include <vector>

#include "AppendOnlyFile.hpp"
#include "Store.hpp"

#include <Foundation/Core/Address.hpp>
#include <Foundation/NBIO/ListenChannel.hpp>
#include <Foundation/NBIO/Session.hpp>
#include <Foundation/Async/Task.hpp>

#include <Application/RESP/RESP.hpp>
#include <Application/Commands.hpp>

namespace KV
{
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

    void run(const Foundation::Core::Address &address);

    Foundation::NBIO::Task<void> serve(const Foundation::Core::Address &address);
    Foundation::NBIO::Task<void> accept_clients(std::unique_ptr<Foundation::NBIO::ListenChannel> listener);
    Foundation::NBIO::Task<void> serve_client(std::shared_ptr<Session> session);

  private:
    Foundation::NBIO::Task<RESP::Object> dispatch(Session &session, KV::Command command);
    Foundation::NBIO::Task<RESP::Object> execute(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_ping(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_get(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_set(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_del(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_exists(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_appendonly(const KV::Command &command);
    Foundation::NBIO::Task<RESP::Object> execute_save(const KV::Command& command);

    bool replay_aof_command(const KV::Command &command);
    bool should_append_to_aof(const KV::Command &command) const noexcept;

    LRUStore<std::string, std::string, HashMap> store_;
    AppendOnlyFile aof_;
};
} // namespace KV
