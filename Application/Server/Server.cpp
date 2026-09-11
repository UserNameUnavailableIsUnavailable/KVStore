#include "Server.hpp"
#include "Backup.hpp"

#include <Foundation/Async/Async.hpp>
#include <Foundation/Buffer.hpp>

#include <Application/Commands.hpp>
#include <Application/RESP/RESP.hpp>
#include <Application/RESP/Receiver.hpp>
#include <Application/RESP/Sender.hpp>


#include <memory>
#include <optional>
#include <spdlog/spdlog.h>
#include <string>
#include <stdexcept>
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
} // namespace

void Server::run(const Foundation::Address &address)
{
    Backup backup;
    if (!backup.load(store_))
    {
        throw std::runtime_error("failed to load RDB snapshot");
    }
    if (!aof_.replay([this](const KV::Command &command) {
            return replay_aof_command(command);
        }))
    {
        throw std::runtime_error("failed to load AOF snapshot");
    }
    Foundation::Async::run(serve(address));
}

Foundation::Async::Task<void> Server::serve(const Foundation::Address &address)
{
    co_await std::move(accept_clients(Foundation::Async::Net::listen_on(address)));
}

Foundation::Async::Task<void> Server::accept_clients(std::unique_ptr<Foundation::Async::ListenService> listener)
{
    while (true)
    {
        auto result = co_await listener->accept();
        if (result.status != Foundation::AcceptStatus::kDone)
        {
            continue;
        }
        Foundation::Async::spawn(serve_client(std::make_shared<Session>(Foundation::Async::Net::establish_with(std::move(result.socket)))));
    }
}

    Foundation::Async::Task<void> Server::serve_client(std::shared_ptr<Session> session)
{
    auto recv_buffer = std::make_unique<::Foundation::Buffer>();
    auto send_buffer = std::make_unique<::Foundation::Buffer>();
    while (true)
    {
        RESP::Receiver receiver(session->transport(), *recv_buffer);
        auto object = co_await receiver.receive();
        if (object) [[likely]]
        {
            const KV::CommandValidation validation = KV::ValidateCommand(*object);
            RESP::Object response = validation ? co_await dispatch(*session, std::move(*validation.command))
                                               : detail::Error(validation.error);
            RESP::Sender sender(session->transport(), *send_buffer, response);
            auto ok = co_await sender.send();
            if (!ok)
            {
                co_return;
            }
        }
        else if (!receiver.decode_error().empty())
        {
            RESP::Object response(RESP::SimpleError{
                .value = receiver.decode_error()
            });
            RESP::Sender sender(session->transport(), *send_buffer, response);
            (void)co_await sender.send();
        }
        else // internal server error
        {
            RESP::Object response(RESP::SimpleError{
                .value = "internal error"
            });
            RESP::Sender sender(session->transport(), *send_buffer, response);
            (void)co_await sender.send();
            co_return;
        }
    }
}

Foundation::Async::Task<RESP::Object> Server::dispatch(Session &session, KV::Command command)
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

Foundation::Async::Task<RESP::Object> Server::execute(const KV::Command &command)
{
    RESP::Object response = detail::Error("ERR command cannot be executed");
    switch (command.type)
    {
    case KV::CommandType::kPing:
        response = co_await execute_ping(command);
        break;
    case KV::CommandType::kGet:
        response = co_await execute_get(command);
        break;
    case KV::CommandType::kSet:
        response = co_await execute_set(command);
        break;
    case KV::CommandType::kDel:
        response = co_await execute_del(command);
        break;
    case KV::CommandType::kExists:
        response = co_await execute_exists(command);
        break;
    case KV::CommandType::kAppendOnly:
        response = co_await execute_appendonly(command);
        break;
    case KV::CommandType::kSave:
        response = co_await execute_save(command);
        break;
    default:
        break;
    }
    if (should_append_to_aof(command) && aof_.enabled())
    {
        co_await aof_.append(command);
    }
    co_return response;
}

Foundation::Async::Task<RESP::Object> Server::execute_ping(const KV::Command &command)
{
    (void)command;
    co_return RESP::Object(RESP::SimpleString{.value = "PONG"});
}

Foundation::Async::Task<RESP::Object> Server::execute_get(const KV::Command &command)
{
    const auto &get = std::get<KV::GetParams>(command.parameters);
    co_return RESP::Object(RESP::BulkString{.value = store_.get(get.key)});
}

Foundation::Async::Task<RESP::Object> Server::execute_set(const KV::Command &command)
{
    const auto &set = std::get<KV::SetParams>(command.parameters);
    store_.set(set.key, set.value);
    co_return RESP::Object(RESP::SimpleString{.value = "OK"});
}

Foundation::Async::Task<RESP::Object> Server::execute_del(const KV::Command &command)
{
    const auto &del = std::get<KV::DelParams>(command.parameters);
    const bool exists = store_.contains(del.key);
    store_.set(del.key, std::nullopt);
    co_return RESP::Object(RESP::Integer{.value = exists ? 1 : 0});
}

Foundation::Async::Task<RESP::Object> Server::execute_exists(const KV::Command &command)
{
    const auto &exists = std::get<KV::ExistsParams>(command.parameters);
    co_return RESP::Object(RESP::Boolean{.value = store_.contains(exists.key)});
}

bool Server::should_append_to_aof(const KV::Command &command) const noexcept
{
    switch (command.type)
    {
    case KV::CommandType::kSet:
    case KV::CommandType::kDel:
    case KV::CommandType::kExpire:
        return true;
    default:
        return false;
    }
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

Foundation::Async::Task<RESP::Object> Server::execute_appendonly(const KV::Command &command)
{
    const auto &appendonly = std::get<KV::AppendOnlyParams>(command.parameters);
    if (appendonly.enabled)
    {
        if (!aof_.enable())
        {
            co_return detail::Error("ERR could not open AOF");
        }
    }
    else
    {
        aof_.disable();
    }
    co_return RESP::Object(RESP::SimpleString{.value = "OK"});
}

Foundation::Async::Task<RESP::Object> Server::execute_save(const KV::Command& command)
{
    (void)command;
    Backup backup;
    if (!co_await backup.save(store_))
    {
        co_return detail::Error("ERR failed to save RDB snapshot");
    }
    co_return RESP::Object(RESP::SimpleString{.value = "OK"});
}
} // namespace KV
