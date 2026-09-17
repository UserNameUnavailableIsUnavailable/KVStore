#include "Socket.hpp"

#include <cerrno>

#if defined(_WIN32)
#include <winsock2.h>
#endif

#if defined(__linux__)
#define IS_SOCKET_ERROR_AGAIN (errno == EAGAIN || errno == EWOULDBLOCK)
#elif defined(_WIN32)
#define IS_SOCKET_ERROR_AGAIN (::WSAGetLastError() == WSAEWOULDBLOCK)
#endif

#if defined(__linux__)
#define IS_SOCKET_ERROR_INTERRUPTED (errno == EINTR)
#elif defined(_WIN32)
#define IS_SOCKET_ERROR_INTERRUPTED (::WSAGetLastError() == WSAEINTR)
#endif
// IS_SOCKET_ERROR_PEER_CLOSED macro checks if the socket has been closed by the
// peer. Note that this error is typically encountered when writing to the peer
// but can also occur during other socket operations. Receiving data from a peer
// does not trigger this error since data are already available in the kernel
// buffer.
#if defined(__linux__)
#define IS_SOCKET_ERROR_PEER_CLOSED (errno == ECONNRESET || errno == ENOTCONN)
#elif defined(_WIN32)
#define IS_SOCKET_ERROR_PEER_CLOSED (::WSAGetLastError() == WSAECONNRESET || ::WSAGetLastError() == WSAENOTCONN)
#endif
#include <system_error>

namespace Foundation::Core
{
namespace
{
#if defined(_WIN32)
int &WinsockRefs()
{
    static int refs = 0;
    return refs;
}

// Reference-counted Winsock lifecycle. The first live socket brings Winsock up,
// the last one tears it down. No-op on Linux.
void EnsureWinsock()
{
    int &refs = WinsockRefs();
    if (refs == 0)
    {
        ::WSADATA data{};
        if (::WSAStartup(MAKEWORD(2, 2), &data) != 0)
        {
            throw std::system_error(std::error_code(static_cast<int>(::WSAGetLastError()), std::system_category()),
                                    "WSAStartup failed");
        }
    }
    ++refs;
}

void ReleaseWinsock()
{
    int &refs = WinsockRefs();
    if (refs > 0 && --refs == 0)
    {
        ::WSACleanup();
    }
}
#else
void EnsureWinsock()
{
}
void ReleaseWinsock()
{
}
#endif
} // namespace

std::error_code Socket::get_last_error()
{
#if defined(__linux__)
    return std::error_code(errno, std::system_category());
#elif defined(_WIN32)
    return std::error_code(static_cast<int>(::WSAGetLastError()), std::system_category());
#endif
}

Socket::Socket(Address::Family family, Type type)
{
    EnsureWinsock();

    int domain = 0;
    int type_ = 0;
    switch (family)
    {
    case Address::Family::kIPv4:
        domain = AF_INET;
        break;
    case Address::Family::kIPv6:
        domain = AF_INET6;
        break;
    default:
        throw std::system_error(std::make_error_code(std::errc::address_family_not_supported),
                                "Socket: unsupported address family");
    }
    switch (type)
    {
    case Type::kStream:
        type_ = SOCK_STREAM;
        break;
    case Type::kDatagram:
        type_ = SOCK_DGRAM;
        break;
    default:
        throw std::system_error(std::make_error_code(std::errc::protocol_not_supported), "Socket: unsupported type");
    }

    // Foundation::Socket is shared in Linux and Windows
    handle_ = ::socket(domain, type_, 0);
    if (handle_ == kInvalidHandle)
    {
        ReleaseWinsock();
        throw std::system_error(get_last_error(), "Socket: failed to create socket");
    }
#if defined(__linux__)
    // this guarantees the socket never leaks to child processes in Linux
    // in Windows, this is not needed because handle inheritance policy is
    // stricter
    ::fcntl(handle_, F_SETFD, FD_CLOEXEC);
#endif
}

Socket Socket::Adopt(Handle handle) noexcept
{
    if (handle == kInvalidHandle)
    {
        return Socket{};
    }
    EnsureWinsock();
    Socket socket;
    socket.handle_ = handle;
    return socket;
}

void Socket::bind(const Address &local)
{
    if (::bind(handle_, local.storage<sockaddr>(), local.length()) < 0)
    {
        throw std::system_error(get_last_error(), "Socket: bind failed");
    }
}

void Socket::listen(int backlog)
{
    if (::listen(handle_, backlog) < 0)
    {
        throw std::system_error(get_last_error(), "Socket: listen failed");
    }
}

Socket Socket::accept(Address &peer)
{
    peer.length() = Address::capacity();
    const Handle client = ::accept(handle_, peer.storage<sockaddr>(), &peer.length());
    if (client == kInvalidHandle)
    {
        throw std::system_error(get_last_error(), "Socket: accept failed");
    }
    return Adopt(client);
}

AcceptResult Socket::accept()
{
    AcceptResult result;
    result.status = AcceptStatus::kPending;
    bool retry = false;

    do
    {
        retry = false;
        result.address.length() = Address::capacity();
        auto handle = ::accept(handle_, result.address.storage<sockaddr>(), &result.address.length());

        if (handle != kInvalidHandle)
        {
            result.status = AcceptStatus::kDone;
            result.socket = Socket::Adopt(handle);
        }
        else
        {
            result.socket = {};
            if (IS_SOCKET_ERROR_AGAIN)
            {
                // pending
            }
            else if (IS_SOCKET_ERROR_INTERRUPTED)
            {
                retry = true;
            }
            else
            {
                result.status = AcceptStatus::kError;
                result.error_code = get_last_error();
            }
        }
    } while (retry);

    return result;
}

ReceiveResult Socket::receive(std::span<char> buffer)
{
    ReceiveResult result{
        .status = ReceiveStatus::kPending,
        .bytes_transferred = 0,
    };
    bool retry = false;

    do
    {
        retry = false;
        auto n = ::recv(handle_, buffer.data(), buffer.size(), 0);
        if (n > 0)
        {
            result.bytes_transferred = static_cast<std::size_t>(n);
            result.status = ReceiveStatus::kDone;
        }
        else if (n == 0)
        {
            result.status = ReceiveStatus::kPeerClosed;
        }
        else if (IS_SOCKET_ERROR_AGAIN)
        {
            result.status = ReceiveStatus::kPending; // stay suspended
        }
        else if (IS_SOCKET_ERROR_INTERRUPTED)
        {
            retry = true;
        }
        else
        {
            result.status = ReceiveStatus::kError;
            result.error_code = get_last_error();
        }
    } while (retry);

    return result;
}

SendResult Socket::send(std::span<const char> buffer)
{
    SendResult result{
        .status = SendStatus::kPending,
        .bytes_transferred = 0,
    };
    bool retry = false;
    do
    {
        retry = false;
        auto n = ::send(handle_, buffer.data(), buffer.size(), MSG_NOSIGNAL);
        if (n >= 0)
        {
            result.bytes_transferred += static_cast<std::size_t>(n);
            // "write flushes all": done only when nothing is left to send.
            result.status = (result.bytes_transferred == buffer.size()) ? SendStatus::kDone : SendStatus::kPending;
        }
        else if (IS_SOCKET_ERROR_AGAIN)
        {
            result.status = SendStatus::kPending;
        }
        else if (IS_SOCKET_ERROR_INTERRUPTED)
        {
            retry = true;
        }
        else if (IS_SOCKET_ERROR_PEER_CLOSED)
        {
            result.status = SendStatus::kPeerClosed;
        }
        else
        {
            result.status = SendStatus::kError;
            result.error_code = get_last_error();
        }
    } while (retry);
    return result;
}

void Socket::connect(const Address &peer)
{
    if (::connect(handle_, peer.storage<sockaddr>(), peer.length()) < 0)
    {
        throw std::system_error(get_last_error(), "Socket: connect failed");
    }
}

void Socket::set_non_blocking(bool enable)
{
#if defined(__linux__)
    const int flags = ::fcntl(handle_, F_GETFL, 0);
    if (flags < 0)
    {
        throw std::system_error(get_last_error(), "Socket: fcntl(F_GETFL) failed");
    }
    const int updated = enable ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    if (::fcntl(handle_, F_SETFL, updated) < 0)
    {
        throw std::system_error(get_last_error(), "Socket: fcntl(F_SETFL) failed");
    }
#elif defined(_WIN32)
    u_long mode = enable ? 1 : 0;
    if (::ioctlsocket(handle_, FIONBIO, &mode) != 0)
    {
        throw std::system_error(LastError(), "Socket: ioctlsocket(FIONBIO) failed");
    }
#endif
    non_blocking_ = enable;
}

template <typename T> void Socket::set_native_option(int level, int option, const T &value)
{
#if defined(_WIN32)
    if (::setsockopt(handle_, level, option, reinterpret_cast<const char *>(&value), static_cast<int>(sizeof(T))) < 0)
#elif defined(__linux__)
    if (::setsockopt(handle_, level, option, &value, static_cast<::socklen_t>(sizeof(T))) < 0)
#endif
    {
        throw std::system_error(get_last_error(), "Socket: setsockopt failed");
    }
}

void Socket::set_reuse_address(bool enable)
{
    const int value = enable ? 1 : 0;
    set_native_option(SOL_SOCKET, SO_REUSEADDR, value);
    reuse_address_ = enable;
}

void Socket::set_reuse_port(bool enable)
{
#if defined(SO_REUSEPORT)
    const int value = enable ? 1 : 0;
    set_native_option(SOL_SOCKET, SO_REUSEPORT, value);
#endif
    reuse_port_ = enable;
}

void Socket::set_keep_alive(bool enable)
{
    const int value = enable ? 1 : 0;
    set_native_option(SOL_SOCKET, SO_KEEPALIVE, value);
    keep_alive_ = enable;
}

void Socket::get_local_address(Address &out) const
{
    out.length() = Address::capacity();
    if (::getsockname(handle_, out.storage<sockaddr>(), &out.length()) < 0)
    {
        throw std::system_error(Socket::get_last_error(), "Socket: getsockname failed");
    }
}

void Socket::get_peer_address(Address &out) const
{
    out.length() = Address::capacity();
    if (::getpeername(handle_, out.storage<sockaddr>(), &out.length()) < 0)
    {
        throw std::system_error(get_last_error(), "Socket: getpeername failed");
    }
}

void Socket::shutdown(ShutdownHow how) noexcept
{
    if (!is_valid())
    {
        return;
    }
    int what = 0;
    switch (how)
    {
#if defined(__linux__)
    case ShutdownHow::kRead:
        what = SHUT_RD;
        break;
    case ShutdownHow::kWrite:
        what = SHUT_WR;
        break;
    case ShutdownHow::kBoth:
        what = SHUT_RDWR;
        break;
#elif defined(_WIN32)
    case ShutdownHow::kRead:
        what = SD_RECEIVE;
        break;
    case ShutdownHow::kWrite:
        what = SD_Send;
        break;
    case ShutdownHow::kBoth:
        what = SD_BOTH;
        break;
#endif
    }
    ::shutdown(handle_, what);
}

void Socket::close() noexcept
{
    if (!is_valid())
    {
        return;
    }
#if defined(__linux__)
    ::close(handle_);
#elif defined(_WIN32)
    ::closesocket(handle_);
#endif
    handle_ = kInvalidHandle;
    ReleaseWinsock();
}

// Explicit instantiation so the template is emitted once in this translation
// unit (callers only use it through the public setters above).
template void Socket::set_native_option<int>(int, int, const int &);
} // namespace Foundation::Core
