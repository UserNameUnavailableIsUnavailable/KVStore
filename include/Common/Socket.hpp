#pragma once

#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <format>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <fcntl.h>

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <winsock2.h>
#else
#error "Unsupported platform"
#endif

#include "Common/Address.hpp"

namespace KV
{
enum class SocketProtocol
{
    kTcp,
    kUdp,
};

#if defined(__linux__)
class LinuxSocket
{
public:
    using HandleType = int;

    LinuxSocket() = default;

    explicit LinuxSocket(SocketProtocol protocol)
    {
        const int type = protocol == SocketProtocol::kTcp ? SOCK_STREAM : SOCK_DGRAM;
        const int native_protocol = protocol == SocketProtocol::kTcp ? IPPROTO_TCP : IPPROTO_UDP;
        handle_ = ::socket(AF_INET, type, native_protocol);
        if (handle_ < 0)
        {
            throw std::system_error(errno, std::system_category(), "failed to create socket");
        }
    }

    LinuxSocket(const LinuxSocket&) = delete;
    LinuxSocket& operator=(const LinuxSocket&) = delete;

    LinuxSocket(LinuxSocket&& other) noexcept :
        handle_(std::exchange(other.handle_, kInvalidHandle))
    {
    }

    LinuxSocket& operator=(LinuxSocket&& other) noexcept
    {
        if (this != &other)
        {
            Close();
            handle_ = std::exchange(other.handle_, kInvalidHandle);
        }
        return *this;
    }

    ~LinuxSocket() noexcept
    {
        Close();
    }

    // Take ownership of a raw fd.  The socket is already open and configured;
    // further queries (local/peer address) go straight to the kernel so no
    // cached state can ever be stale.
    static LinuxSocket Adopt(HandleType handle) noexcept
    {
        LinuxSocket socket;
        socket.handle_ = handle;
        return socket;
    }

    HandleType GetNativeHandle() const noexcept
    {
        return handle_;
    }

    bool IsOpen() const noexcept
    {
        return handle_ != kInvalidHandle;
    }

    void SetReuseAddress(bool enabled = true)
    {
        const int value = enabled ? 1 : 0;
        if (::setsockopt(handle_, SOL_SOCKET, SO_REUSEADDR, &value, sizeof(value)) < 0)
        {
            throw std::system_error(errno, std::system_category(), "failed to set SO_REUSEADDR");
        }
    }

    void Bind(std::uint16_t port)
    {
        const ::sockaddr_in address{
            .sin_family = AF_INET,
            .sin_port = htons(port),
            .sin_addr = {.s_addr = htonl(INADDR_ANY)},
            .sin_zero = {},
        };
        if (::bind(handle_, reinterpret_cast<const ::sockaddr*>(&address), sizeof(address)) < 0)
        {
            throw std::system_error(errno, std::system_category(), "failed to bind socket");
        }
    }

    // Populate *out* with the socket's local endpoint.  If the kernel
    // assigned a port (bind(0)), this is how you discover what it chose.
    void GetLocalAddress(Address& out) const
    {
        out.Reset();
        out.GetSize() = out.GetCapacity();
        if (::getsockname(handle_, &out.GetStorage<::sockaddr>(), &out.GetSize()) < 0)
        {
            throw std::system_error(errno, std::system_category(), "getsockname failed");
        }
    }

    // Populate *out* with the peer endpoint.  For an accepted connection this
    // is equivalent to the address the kernel already wrote through accept().
    void GetPeerAddress(Address& out) const
    {
        out.Reset();
        out.GetSize() = out.GetCapacity();
        if (::getpeername(handle_, &out.GetStorage<::sockaddr>(), &out.GetSize()) < 0)
        {
            throw std::system_error(errno, std::system_category(), "getpeername failed");
        }
    }

    void Connect(const std::string& host, std::uint16_t port)
    {
        ::in_addr address{};
        if (::inet_pton(AF_INET, host.c_str(), &address) != 1)
        {
            throw std::invalid_argument("host must be a valid IPv4 address");
        }

        const ::sockaddr_in peer{
            .sin_family = AF_INET,
            .sin_port = htons(port),
            .sin_addr = address,
            .sin_zero = {},
        };
        if (::connect(handle_, reinterpret_cast<const ::sockaddr*>(&peer), sizeof(peer)) < 0)
        {
            throw std::system_error(errno, std::system_category(), "failed to connect socket");
        }
    }

    void Listen(int backlog = SOMAXCONN)
    {
        if (::listen(handle_, backlog) < 0)
        {
            throw std::system_error(errno, std::system_category(), "failed to listen on socket");
        }
    }

    void SetNonBlocking()
    {
        const int flags = ::fcntl(handle_, F_GETFL, 0);
        if (flags < 0 || ::fcntl(handle_, F_SETFL, flags | O_NONBLOCK) < 0)
        {
            throw std::runtime_error(std::format("failed to enable nonblocking socket mode, errno: {}", errno));
        }
    }

    void Swap(LinuxSocket& other) noexcept
    {
        std::swap(handle_, other.handle_);
    }

private:
    static constexpr HandleType kInvalidHandle = -1;

    void Close() noexcept
    {
        if (handle_ != kInvalidHandle)
        {
            ::close(handle_);
            handle_ = kInvalidHandle;
        }
    }

    HandleType handle_ = kInvalidHandle;
};

using Socket = LinuxSocket;
#elif defined(_WIN32)
class WindowsSocket;
using Socket = WindowsSocket;
#endif
} // namespace KV
