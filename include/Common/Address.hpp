#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#elif defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#error "Unsupported platform"
#endif

namespace KV
{
class Address
{
public:
#if defined(__linux__)
    using SizeType = ::socklen_t;
#elif defined(_WIN32)
    using SizeType = int;
#endif

    using StorageType = ::sockaddr_storage;
    using GeneralStorageType = ::sockaddr;
    using IPv4StorageType = ::sockaddr_in;
    using IPv6StorageType = ::sockaddr_in6;

    Address() noexcept
    {
        Reset();
    }

    IPv4StorageType& GetIPv4Storage() noexcept
    {
        return GetStorage<IPv4StorageType>();
    }

    const IPv4StorageType& GetIPv4Storage() const noexcept
    {
        return GetStorage<IPv4StorageType>();
    }

    IPv6StorageType& GetIPv6Storage() noexcept
    {
        return GetStorage<IPv6StorageType>();
    }

    const IPv6StorageType& GetIPv6Storage() const noexcept
    {
        return GetStorage<IPv6StorageType>();
    }

    template <typename T = StorageType>
    T& GetStorage() noexcept
    {
        return *reinterpret_cast<T*>(&storage_);
    }

    template <typename T = StorageType>
    const T& GetStorage() const noexcept
    {
        return *reinterpret_cast<const T*>(&storage_);
    }

    static constexpr SizeType GetCapacity() noexcept
    {
        return sizeof(StorageType);
    }

    SizeType& GetSize() noexcept
    {
        return size_;
    }

    SizeType GetSize() const noexcept
    {
        return size_;
    }

    void Reset() noexcept
    {
        storage_ = {};
        size_ = 0;
    }

    std::string GetIP() const
    {
        char buffer[INET6_ADDRSTRLEN] = {};
        if (storage_.ss_family == AF_INET)
        {
            // A well-formed IPv4 endpoint measures exactly sizeof(sockaddr_in);
            // anything shorter means the address bytes were never filled in.
            if (size_ < sizeof(sockaddr_in)) [[unlikely]]
            {
                throw std::runtime_error("invalid IPv4 address size");
            }
            const auto* ipv4 = reinterpret_cast<const ::sockaddr_in*>(&storage_);
            ::inet_ntop(AF_INET, &ipv4->sin_addr, buffer, sizeof(buffer));
        }
        else if (storage_.ss_family == AF_INET6)
        {
            if (size_ < sizeof(sockaddr_in6)) [[unlikely]]
            {
                throw std::runtime_error("invalid IPv6 address size");
            }
            const auto* ipv6 = reinterpret_cast<const ::sockaddr_in6*>(&storage_);
            ::inet_ntop(AF_INET6, &ipv6->sin6_addr, buffer, sizeof(buffer));
        }
        return buffer;
    }

    std::uint16_t GetPort() const
    {
        if (storage_.ss_family == AF_INET)
        {
            if (size_ < sizeof(sockaddr_in)) [[unlikely]]
            {
                throw std::runtime_error("invalid IPv4 address size");
            }
            return ntohs(reinterpret_cast<const ::sockaddr_in*>(&storage_)->sin_port);
        }
        if (storage_.ss_family == AF_INET6)
        {
            if (size_ < sizeof(sockaddr_in6)) [[unlikely]]
            {
                throw std::runtime_error("invalid IPv6 address size");
            }
            return ntohs(reinterpret_cast<const ::sockaddr_in6*>(&storage_)->sin6_port);
        }
        return 0;
    }

private:
    ::sockaddr_storage storage_;
    SizeType size_ = 0;
};
} // namespace KV