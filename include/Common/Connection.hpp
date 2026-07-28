#pragma once

#include <unistd.h>
#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <arpa/inet.h>
#include <netinet/in.h>

namespace KV
{
class Connection
{
public:
    Connection() = default;
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&& other) noexcept :
        socket_handle_(std::exchange(other.socket_handle_, -1)),
        address_(other.address_),
        address_length_(other.address_length_),
        timeout_(other.timeout_)
    {
    }

    Connection& operator=(Connection&& other) noexcept
    {
        if (this != &other)
        {
            Close();
            socket_handle_ = std::exchange(other.socket_handle_, -1);
            address_ = other.address_;
            address_length_ = other.address_length_;
            timeout_ = other.timeout_;
        }
        return *this;
    }

    ~Connection()
    {
        Close();
    }

    int GetFileDescriptor() const
    {
        return socket_handle_;
    }

    void SetFileDescriptor(int fd)
    {
        if (socket_handle_ >= 0 && socket_handle_ != fd)
        {
            ::close(socket_handle_);
        }
        socket_handle_ = fd;
    }

    void Close()
    {
        if (socket_handle_ >= 0)
        {
            ::close(socket_handle_);
            socket_handle_ = -1;
        }
    }

    void Reset()
    {
        Close();
        address_ = {};
        address_length_ = sizeof(address_);
    }

    // Connection control ----------------------------------------------------

    // Peer address of the connection, derived from the stored socket address.
    // Returns an empty string when the peer address is unknown.
    std::string GetPeerAddress() const
    {
        char buffer[INET6_ADDRSTRLEN] = {};
        if (address_.ss_family == AF_INET)
        {
            const auto* v4 = reinterpret_cast<const ::sockaddr_in*>(&address_);
            ::inet_ntop(AF_INET, &v4->sin_addr, buffer, sizeof(buffer));
        }
        else if (address_.ss_family == AF_INET6)
        {
            const auto* v6 = reinterpret_cast<const ::sockaddr_in6*>(&address_);
            ::inet_ntop(AF_INET6, &v6->sin6_addr, buffer, sizeof(buffer));
        }
        return std::string(buffer);
    }

    // Peer port of the connection, or 0 when unknown.
    std::uint16_t GetPeerPort() const
    {
        if (address_.ss_family == AF_INET)
        {
            return ntohs(reinterpret_cast<const ::sockaddr_in*>(&address_)->sin_port);
        }
        if (address_.ss_family == AF_INET6)
        {
            return ntohs(reinterpret_cast<const ::sockaddr_in6*>(&address_)->sin6_port);
        }
        return 0;
    }

    // Connection timeout metadata. A zero timeout means "no timeout".
    void SetTimeout(std::chrono::milliseconds timeout)
    {
        timeout_ = timeout;
    }

    std::chrono::milliseconds GetTimeout() const
    {
        return timeout_;
    }

    template <typename F>
    auto WithMutableAcceptContext(F&& f)
    {
        address_length_ = sizeof(address_);
        return f(&address_, &address_length_);
    }

private:
    std::uintptr_t socket_handle_ = -1;
    ::sockaddr_storage address_{};
    ::socklen_t address_length_ = sizeof(address_);
    std::chrono::milliseconds timeout_{0};
};
} // namespace KV