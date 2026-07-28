#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <memory_resource>
#include <string>
#include <unordered_map>

#include "Common/Command.hpp"
#include "Common/Result.hpp"
#include "Common/Storage.hpp"

namespace KV
{
class Server
{
public:
    Server();
    void Bind(std::uint16_t port);

    // Select the memory resource every session allocates through, realising the
    // pluggable Memory Pooling Layer. The resource must outlive the server, and
    // must be chosen before Run() so all sessions share the same pool. Leaving
    // it unset keeps the standard new/delete resource.
    void SetMemoryResource(std::pmr::memory_resource* resource)
    {
        memory_resource_ = resource;
    }

    virtual void Run() = 0;
    virtual ~Server() noexcept;

protected:
    Result Execute(const Command& command);
    std::uintptr_t GetSocketHandle() const;
    std::uint16_t GetPort() const;
    std::pmr::memory_resource* GetMemoryResource() const
    {
        return memory_resource_;
    }

private:
    using CommandHandler = std::function<Result(const Command&)>;
    void RegisterHandlers();
    std::uintptr_t server_socket_handle = -1;
    std::uint16_t port_ = 0;
    std::unordered_map<std::string, CommandHandler> handlers_;
    std::unique_ptr<Storage<std::string, std::string>> storage_;
    std::pmr::memory_resource* memory_resource_ = std::pmr::get_default_resource();
};
} // namespace KV
