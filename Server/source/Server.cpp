#include "Server/Server.hpp"

#include <unistd.h>
#include <cerrno>
#include <cstdint>
#include <format>
#include <optional>
#include <stdexcept>

#include <liburing/io_uring.h>
#include <sys/socket.h>
#include <netinet/in.h>

namespace KV
{

Server::Server()
{
	// Default cache: hash-indexed with LRU eviction. The storage structure and
	// eviction strategy can later be selected from the command line.
	storage_ = MakeStorage(StorageStructure::kHash, EvictionStrategy::kLru, 32, memory_resource_);

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        throw std::runtime_error(std::format("failed to create socket, errno: {}", errno));
    }
	
    int yes = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
    {
		close(fd);
        throw std::runtime_error(std::format("failed to set SO_REUSEADDR, errno: {}", errno));
    }

	server_socket_handle = static_cast<std::uintptr_t>(fd);
	RegisterHandlers();
}

void Server::Bind(std::uint16_t port)
{
	if (port_ != 0)
	{
		throw std::runtime_error(std::format("server already bound to port {}", port_));
	}
    ::sockaddr_in addr {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {
            .s_addr = htonl(INADDR_ANY),
        },
        .sin_zero = {}
    };
    int ret = ::bind(static_cast<int>(server_socket_handle), reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (ret < 0)
    {
        throw std::runtime_error((std::format("failed to bind to port {}, errno: {}", port, errno)));
    }
    port_ = port;
}

Server::~Server() noexcept
{
	if (server_socket_handle >= 0)
	{
		::close(static_cast<int>(server_socket_handle));
	}
}

std::uintptr_t Server::GetSocketHandle() const
{
	return static_cast<std::uintptr_t>(server_socket_handle);
}

std::uint16_t Server::GetPort() const
{
	return port_;
}

Result Server::Execute(const Command& command)
{
    auto it = handlers_.find(command.GetName());
    if (it == handlers_.end())
    {
        return Result(false, "unknown command", "");
    }
    return it->second(command);
}

void Server::RegisterHandlers()
{
    handlers_["GET"] = [this](const Command& command) -> Result
    {
        if (command.GetArguments().size() != 1)
        {
            return Result(false, "usage: GET key", "");
        }
        auto value = storage_->Get(command.GetArguments()[0]);
        if (!value.has_value())
        {
            return Result(false, "key not found", "");
        }
        return Result(true, "", *value);
    };

    handlers_["SET"] = [this](const Command& command) -> Result
    {
        if (command.GetArguments().size() != 2)
        {
            return Result(false, "usage: SET key value", "");
        }
        storage_->Set(command.GetArguments()[0], command.GetArguments()[1]);
        return Result(true, "", "");
    };

    handlers_["DELETE"] = [this](const Command& command) -> Result
    {
        if (command.GetArguments().size() != 1)
        {
            return Result(false, "usage: DELETE key", "");
        }
        const std::string& key = command.GetArguments()[0];
        if (!storage_->Exists(key))
        {
            return Result(false, "key not found", "");
        }
        storage_->Set(key, std::nullopt);
        return Result(true, "", "");
    };

    handlers_["EXISTS"] = [this](const Command& command) -> Result
    {
        if (command.GetArguments().size() != 1)
        {
            return Result(false, "usage: EXISTS key", "");
        }
        return Result(true, "", storage_->Exists(command.GetArguments()[0]) ? "YES" : "NO");
    };
}

} // namespace KV
