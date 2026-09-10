#include "Address.hpp"
#include <stdexcept>

namespace Foundation
{
Address Address::from_ipv4(std::string_view ip, std::uint16_t port)
{
    Address address;
    auto &v4 = *reinterpret_cast<::sockaddr_in *>(&address.storage_);
    v4.sin_family = AF_INET;
    v4.sin_port = htons(port);
    if (::inet_pton(AF_INET, std::string(ip).c_str(), &v4.sin_addr) != 1)
    {
        throw std::invalid_argument("Address::FromIPv4: invalid IPv4 address");
    }
    address.length_ = sizeof(::sockaddr_in);
    return address;
}

Address Address::from_ipv6(std::string_view ip, std::uint16_t port)
{
    Address address;
    auto &v6 = *reinterpret_cast<::sockaddr_in6 *>(&address.storage_);
    v6.sin6_family = AF_INET6;
    v6.sin6_port = htons(port);
    if (::inet_pton(AF_INET6, std::string(ip).c_str(), &v6.sin6_addr) != 1)
    {
        throw std::invalid_argument("Address::FromIPv6: invalid IPv6 address");
    }
    address.length_ = sizeof(::sockaddr_in6);
    return address;
}

Address::Family Address::family() const noexcept
{
    switch (storage_.ss_family)
    {
    case AF_INET:
        return Family::kIPv4;
    case AF_INET6:
        return Family::kIPv6;
    default:
        return Family::kUnsupported;
    }
}

std::string Address::ip() const
{
    if (storage_.ss_family == AF_INET)
    {
        char buffer[INET_ADDRSTRLEN] = {};
        const auto *v4 = reinterpret_cast<const ::sockaddr_in *>(&storage_);
        if (::inet_ntop(AF_INET, &v4->sin_addr, buffer, sizeof(buffer)) == nullptr)
        {
            throw std::runtime_error("Address::IP: inet_ntop failed for IPv4 endpoint");
        }
        return buffer;
    }
    if (storage_.ss_family == AF_INET6)
    {
        char buffer[INET6_ADDRSTRLEN] = {};
        const auto *v6 = reinterpret_cast<const ::sockaddr_in6 *>(&storage_);
        if (::inet_ntop(AF_INET6, &v6->sin6_addr, buffer, sizeof(buffer)) == nullptr)
        {
            throw std::runtime_error("Address::IP: inet_ntop failed for IPv6 endpoint");
        }
        return buffer;
    }
    return {};
}

std::uint16_t Address::port() const noexcept
{
    if (storage_.ss_family == AF_INET)
    {
        return ntohs(reinterpret_cast<const ::sockaddr_in *>(&storage_)->sin_port);
    }
    if (storage_.ss_family == AF_INET6)
    {
        return ntohs(reinterpret_cast<const ::sockaddr_in6 *>(&storage_)->sin6_port);
    }
    return 0;
}
} // namespace Foundation
