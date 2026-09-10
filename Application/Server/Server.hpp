#pragma once

#include <memory>
#include <string>
#include <vector>

#include "AppendOnlyFile.hpp"
#include "Store.hpp"

#include <Foundation/Address.hpp>
#include <Foundation/Async/ListenService.hpp>
#include <Foundation/Async/Session.hpp>
#include <Foundation/Async/Task.hpp>

#include <Application/RESP/RESP.hpp>
#include <Application/Commands.hpp>

namespace KV
{
class Session
{
  public:
    explicit Session(std::shared_ptr<Foundation::Async::Session> transport) : transport_(std::move(transport))
    {
    }

    Foundation::Async::Session &transport() const noexcept
    {
        return *transport_;
    }

    bool is_multi{false};
    std::vector<KV::Command> queued_commands;

  private:
    std::shared_ptr<Foundation::Async::Session> transport_;
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

    void run(const Foundation::Address &address);

    Foundation::Async::Task<void> serve(const Foundation::Address &address);
    Foundation::Async::Task<void> accept_clients(std::unique_ptr<Foundation::Async::ListenService> listener);
    Foundation::Async::Task<void> serve_client(std::shared_ptr<Session> session);

  private:
    Foundation::Async::Task<RESP::Object> dispatch(Session &session, KV::Command command);
    Foundation::Async::Task<RESP::Object> execute(const KV::Command &command);
    Foundation::Async::Task<RESP::Object> execute_ping(const KV::Command &command);
    Foundation::Async::Task<RESP::Object> execute_get(const KV::Command &command);
    Foundation::Async::Task<RESP::Object> execute_set(const KV::Command &command);
    Foundation::Async::Task<RESP::Object> execute_del(const KV::Command &command);
    Foundation::Async::Task<RESP::Object> execute_exists(const KV::Command &command);
    Foundation::Async::Task<RESP::Object> execute_appendonly(const KV::Command &command);
    Foundation::Async::Task<RESP::Object> execute_save(const KV::Command& command);

    bool replay_aof_command(const KV::Command &command);
    bool should_append_to_aof(const KV::Command &command) const noexcept;

    LRUStore<std::string, std::string, HashMap> store_;
    AppendOnlyFile aof_;
};
} // namespace KV