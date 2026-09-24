#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#if defined(__linux__)
#include <arpa/inet.h>
#include <sys/socket.h>
#elif defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#error "Unsupported platform"
#endif

namespace Foundation::Core
{
// A thin, copyable wrapper around `sockaddr_storage`. It owns no sockets and
// only describes an endpoint (IPv4 or IPv6). All platform specifics are hidden
// behind a small vocabulary: build it from a textual IP + port, or let the
// kernel fill it in through `Bind`/`Accept`/`GetPeerSocketAddress`.
class SocketAddress
{
  public:
    enum class Family
    {
        kIPv4,
        kIPv6,
        kUnsupported
    };

    SocketAddress() noexcept = default;

    // Build a well-formed endpoint. Throws `std::invalid_argument` when the
    // textual address cannot be parsed by `inet_pton`.
    static SocketAddress from_v4(std::string_view ip_str, std::uint16_t port);

    static SocketAddress from_v6(std::string_view ip_str, std::uint16_t port);

    Family family() const noexcept;

    bool is_valid() const noexcept
    {
        return storage_.ss_family != AF_UNSPEC;
    }

    std::string ip() const;

    std::uint16_t port() const noexcept;

    template <typename T = ::sockaddr> T *storage() noexcept
    {
        return reinterpret_cast<T *>(&storage_);
    }

    template <typename T = ::sockaddr> const T *storage() const noexcept
    {
        return reinterpret_cast<const T *>(&storage_);
    }

    ::socklen_t &length() noexcept
    {
        return length_;
    }

    const ::socklen_t &length() const noexcept
    {
        return length_;
    }

    static constexpr ::socklen_t capacity() noexcept
    {
        return sizeof(storage_);
    }

  private:
    ::sockaddr_storage storage_{};
    ::socklen_t length_ = 0;
};
} // namespace Foundation::Core
