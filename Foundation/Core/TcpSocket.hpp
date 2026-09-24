#pragma once

#include <span>
#include <system_error>
#include <utility>

#include "SocketAddress.hpp"
#include "Expected.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#error "Unsupported platform"
#endif

namespace Foundation::Core
{
class TcpSocket
{
  public:
    enum class Type
    {
        kStream,
        kDatagram,
    };
    enum class ShutdownHow
    {
        kRead,
        kWrite,
        kBoth
    };

    TcpSocket() noexcept = default;

    // creates a new socket of the given family/type. Throws `std::system_error`
    // if the underlying `socket()` call fails.
    TcpSocket(SocketAddress::Family family, Type type);

    TcpSocket(const TcpSocket &) = delete;
    TcpSocket &operator=(const TcpSocket &) = delete;

    TcpSocket(TcpSocket &&other) noexcept
    {
        std::swap(handle_, other.handle_);
    }

    TcpSocket &operator=(TcpSocket &&other) noexcept
    {
        if (this != &other)
        {
            close();
            handle_ = std::exchange(other.handle_, kInvalidHandle);
        }
        return *this;
    }

    ~TcpSocket() noexcept
    {
        close();
    }

    [[nodiscard]] static TcpSocket adopt(std::uintptr_t handle) noexcept;

    std::uintptr_t native_handle() const noexcept
    {
        return handle_;
    }

    void swap(TcpSocket &other) noexcept
    {
        std::swap(handle_, other.handle_);
    }

    bool is_valid() const noexcept
    {
        return handle_ != kInvalidHandle;
    }

    expected<void, std::error_code> bind(const SocketAddress &local) noexcept;
    expected<void, std::error_code> listen(int backlog = 4096) noexcept;
    expected<std::pair<TcpSocket, SocketAddress>, std::error_code> accept() noexcept;
    expected<std::size_t, std::error_code> receive(std::span<char> buffer) noexcept;
    expected<std::size_t, std::error_code> send(std::span<const char> buffer) noexcept;
    expected<void, std::error_code> connect(const SocketAddress &peer) noexcept;

    expected<void, std::error_code> non_blocking(bool toggle = true) noexcept;
    expected<void, std::error_code> reuse_address(bool toggle = true) noexcept;
    expected<void, std::error_code> reuse_port(bool toggle = true) noexcept;
    expected<void, std::error_code> keep_alive(bool toggle = true) noexcept;

    expected<SocketAddress, std::error_code> local_address() const noexcept;
    expected<SocketAddress, std::error_code> peer_address() const noexcept;

    void shutdown(ShutdownHow how = ShutdownHow::kBoth) noexcept;
    void close() noexcept;

  private:
    template <typename T> std::error_code set_native_option(int level, int option, const T &value) noexcept;
    static std::error_code last_error() noexcept;

    constexpr static std::uintptr_t kInvalidHandle{ static_cast<std::uintptr_t>(-1) };
    std::uintptr_t handle_ = kInvalidHandle;
};
} // namespace Foundation::Core
