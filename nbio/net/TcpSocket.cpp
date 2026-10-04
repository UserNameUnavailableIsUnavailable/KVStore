#include "TcpSocket.hpp"

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

#if defined(__linux__)
#define IS_SOCKET_ERROR_PEER_CLOSED (errno == ECONNRESET || errno == ENOTCONN)
#elif defined(_WIN32)
#define IS_SOCKET_ERROR_PEER_CLOSED (::WSAGetLastError() == WSAECONNRESET || ::WSAGetLastError() == WSAENOTCONN)
#endif
#include <system_error>

namespace nbio::net {
namespace {
#if defined(_WIN32)
int& WinsockRefs() {
    static int refs = 0;
    return refs;
}

void EnsureWinsock() {
    int& refs = WinsockRefs();
    if (refs == 0) {
        ::WSADATA data{};
        if (::WSAStartup(MAKEWORD(2, 2), &data) != 0) {
            throw std::system_error(std::error_code(static_cast<int>(::WSAGetLastError()), std::system_category()),
                                    "WSAStartup failed");
        }
    }
    ++refs;
}

void ReleaseWinsock() {
    int& refs = WinsockRefs();
    if (refs > 0 && --refs == 0) {
        ::WSACleanup();
    }
}
#else
void EnsureWinsock() {}
void ReleaseWinsock() {}
#endif
}  // namespace

std::error_code TcpSocket::last_error() noexcept {
#if defined(__linux__)
    return std::error_code(errno, std::system_category());
#elif defined(_WIN32)
    return std::error_code(static_cast<int>(::WSAGetLastError()), std::system_category());
#endif
}

TcpSocket::TcpSocket(Address::Family family, Type type) {
    EnsureWinsock();

    int domain = 0;
    int type_ = 0;
    switch (family) {
        case Address::Family::kIPv4:
            domain = AF_INET;
            break;
        case Address::Family::kIPv6:
            domain = AF_INET6;
            break;
        default:
            throw std::system_error(std::make_error_code(std::errc::address_family_not_supported),
                                    "TcpSocket: unsupported address family");
    }
    switch (type) {
        case Type::kStream:
            type_ = SOCK_STREAM;
            break;
        case Type::kDatagram:
            type_ = SOCK_DGRAM;
            break;
        default:
            throw std::system_error(std::make_error_code(std::errc::protocol_not_supported),
                                    "TcpSocket: unsupported type");
    }

    handle_ = ::socket(domain, type_, 0);
    if (handle_ == kInvalidHandle) {
        ReleaseWinsock();
        throw std::system_error(last_error(), "TcpSocket: failed to create socket");
    }
#if defined(__linux__)
    ::fcntl(handle_, F_SETFD, FD_CLOEXEC);
#endif
}

TcpSocket TcpSocket::Adopt(std::uintptr_t handle) noexcept {
    if (handle == kInvalidHandle) {
        return TcpSocket{};
    }
    EnsureWinsock();
    TcpSocket socket;
    socket.handle_ = handle;
    return socket;
}

utility::expected<void, std::error_code> TcpSocket::Bind(const Address& local) noexcept {
    if (::bind(handle_, local.storage<sockaddr>(), local.length()) < 0) {
        return utility::unexpected(last_error());
    }
    return {};
}

utility::expected<void, std::error_code> TcpSocket::Listen(int backlog) noexcept {
    if (::listen(handle_, backlog) < 0) {
        return utility::unexpected<std::error_code>(last_error());
    }
    return {};
}

utility::expected<std::pair<TcpSocket, Address>, std::error_code> TcpSocket::Accept() noexcept {
    Address address;
    while (true) {
        address.length() = Address::capacity();
        auto handle = ::accept(handle_, address.storage<sockaddr>(), &address.length());

        if (static_cast<std::uintptr_t>(handle) != kInvalidHandle) {
            return std::pair{TcpSocket::Adopt(handle), address};
        }
        if (IS_SOCKET_ERROR_AGAIN) {
            return utility::unexpected<std::error_code>(std::make_error_code(std::errc::operation_would_block));
        }
        if (IS_SOCKET_ERROR_INTERRUPTED) {
            continue;
        }
        return utility::unexpected<std::error_code>(last_error());
    }
}

utility::expected<std::size_t, std::error_code> TcpSocket::Receive(std::span<char> buffer) noexcept {
    while (true) {
        const auto n = ::recv(handle_, buffer.data(), buffer.size(), 0);
        if (n > 0) {
            return static_cast<std::size_t>(n);
        }
        if (n == 0) {
            return 0;
        }
        if (IS_SOCKET_ERROR_AGAIN) {
            return utility::unexpected<std::error_code>(std::make_error_code(std::errc::operation_would_block));
        }
        if (IS_SOCKET_ERROR_INTERRUPTED) {
            continue;
        }
        return utility::unexpected<std::error_code>(last_error());
    }
}

utility::expected<std::size_t, std::error_code> TcpSocket::Send(std::span<const char> buffer) noexcept {
    while (true) {
        const auto n = ::send(handle_, buffer.data(), buffer.size(), MSG_NOSIGNAL);
        if (n >= 0) {
            return static_cast<std::size_t>(n);
        }
        if (IS_SOCKET_ERROR_AGAIN) {
            return utility::unexpected<std::error_code>(std::make_error_code(std::errc::operation_would_block));
        }
        if (IS_SOCKET_ERROR_INTERRUPTED) {
            continue;
        }
        return utility::unexpected<std::error_code>(last_error());
    }
}

utility::expected<void, std::error_code> TcpSocket::Connect(const Address& peer) noexcept {
    if (::connect(handle_, peer.storage<sockaddr>(), peer.length()) < 0) {
        return utility::unexpected<std::error_code>(last_error());
    }
    return {};
}

utility::expected<void, std::error_code> TcpSocket::NonBlocking(bool toggle) noexcept {
#if defined(__linux__)
    const int flags = ::fcntl(handle_, F_GETFL, 0);
    if (flags < 0) {
        return utility::unexpected<std::error_code>(last_error());
    }
    const int updated = toggle ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    if (::fcntl(handle_, F_SETFL, updated) < 0) {
        return utility::unexpected<std::error_code>(last_error());
    }
#elif defined(_WIN32)
    u_long mode = toggle ? 1 : 0;
    if (::ioctlsocket(handle_, FIOnbio, &mode) != 0) {
        return utility::unexpected<std::error_code>(last_error());
    }
#endif
    return {};
}

utility::expected<void, std::error_code> TcpSocket::TakeError() const noexcept {
    int pending = 0;
#if defined(_WIN32)
    int length = static_cast<int>(sizeof(pending));
    if (::getsockopt(handle_, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&pending), &length) < 0)
#elif defined(__linux__)
    ::socklen_t length = static_cast<::socklen_t>(sizeof(pending));
    if (::getsockopt(handle_, SOL_SOCKET, SO_ERROR, &pending, &length) < 0)
#endif
    {
        return utility::unexpected<std::error_code>(last_error());
    }
    if (pending != 0) {
        return utility::unexpected<std::error_code>({pending, std::system_category()});
    }
    return {};
}

template <typename T>
std::error_code TcpSocket::SetNativeOption(int level, int option, const T& value) noexcept {
#if defined(_WIN32)
    if (::setsockopt(handle_, level, option, reinterpret_cast<const char*>(&value), static_cast<int>(sizeof(T))) < 0)
#elif defined(__linux__)
    if (::setsockopt(handle_, level, option, &value, static_cast<::socklen_t>(sizeof(T))) < 0)
#endif
    {
        return last_error();
    }
    return {};
}

utility::expected<void, std::error_code> TcpSocket::ReuseAddress(bool toggle) noexcept {
    const int value = toggle ? 1 : 0;
    if (auto error = SetNativeOption(SOL_SOCKET, SO_REUSEADDR, value)) {
        return utility::unexpected<std::error_code>(std::move(error));
    }
    return {};
}

utility::expected<void, std::error_code> TcpSocket::ReusePort(bool toggle) noexcept {
#if defined(SO_REUSEPORT)
    const int value = toggle ? 1 : 0;
    if (auto error = SetNativeOption(SOL_SOCKET, SO_REUSEPORT, value)) {
        return utility::unexpected<std::error_code>(std::move(error));
    }
#endif
    return {};
}

utility::expected<void, std::error_code> TcpSocket::KeepAlive(bool enable) noexcept {
    const int value = enable ? 1 : 0;
    if (auto error = SetNativeOption(SOL_SOCKET, SO_KEEPALIVE, value)) {
        return utility::unexpected<std::error_code>(std::move(error));
    }
    return {};
}

utility::expected<Address, std::error_code> TcpSocket::GetLocalAddress() const noexcept {
    Address address;
    address.length() = Address::capacity();
    if (::getsockname(handle_, address.storage<sockaddr>(), &address.length()) < 0) {
        return utility::unexpected<std::error_code>(TcpSocket::last_error());
    }
    return address;
}

utility::expected<Address, std::error_code> TcpSocket::GetPeerAddress() const noexcept {
    Address address;
    address.length() = Address::capacity();
    if (::getpeername(handle_, address.storage<sockaddr>(), &address.length()) < 0) {
        return utility::unexpected<std::error_code>(TcpSocket::last_error());
    }
    return address;
}

void TcpSocket::Shutdown(ShutdownHow how) noexcept {
    if (!IsValid()) {
        return;
    }
    int what = 0;
    switch (how) {
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

void TcpSocket::Close() noexcept {
    if (!IsValid()) {
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
}  // namespace nbio::net
