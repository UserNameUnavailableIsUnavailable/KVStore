#pragma once

#include <unistd.h>
#include <cstdint>
#include <utility>
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
        address_length_(other.address_length_)
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
        address_length_ = sizeof(address_);
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
};
} // namespace KV