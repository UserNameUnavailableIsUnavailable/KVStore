#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>

#include "Common/Command.hpp"
#include "Common/LRUCache.hpp"
#include "Common/Result.hpp"

namespace KV
{
class Server
{
public:
    Server();
    void Bind(std::uint16_t port);
    virtual void Run() = 0;
    virtual ~Server() noexcept;

protected:
    Result Execute(const Command& command);
    std::uintptr_t GetSocketHandle() const;
    std::uint16_t GetPort() const;

private:
    using CommandHandler = std::function<Result(const Command&)>;
    void RegisterHandlers();
    std::uintptr_t server_socket_handle = -1;
    std::uint16_t port_ = 0;
    std::unordered_map<std::string, CommandHandler> handlers_;
	LRUCache<std::string> lru_cache_;
};
} // namespace KV
