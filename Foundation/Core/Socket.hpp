#pragma once

#include <span>
#include <system_error>
#include <utility>

#include "Address.hpp"

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
enum class ReceiveStatus
{
    kDone,
    kPending,
    kPeerClosed,
    kError,
};

struct ReceiveResult
{
    ReceiveStatus status{ReceiveStatus::kPending};
    std::size_t bytes_received{0};
    std::error_code error_code{};
};

enum class SendStatus
{
    kDone,
    kPending,
    kPeerClosed,
    kError,
};

struct SendResult
{
    SendStatus status{SendStatus::kPending};
    std::size_t bytes_sent{0};
    std::error_code error_code{};
};

enum class AcceptStatus
{
    kDone,
    kPending,
    kPeerClosed,
    kError,
};

struct AcceptResult;

class Socket
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

    Socket() noexcept = default;

    // creates a new socket of the given family/type. Throws `std::system_error`
    // if the underlying `socket()` call fails.
    Socket(Address::Family family, Type type);

    Socket(const Socket &) = delete;
    Socket &operator=(const Socket &) = delete;

    Socket(Socket &&other) noexcept
    {
        std::swap(handle_, other.handle_);
    }

    Socket &operator=(Socket &&other) noexcept
    {
        if (this != &other)
        {
            close();
            handle_ = std::exchange(other.handle_, kInvalidHandle);
        }
        return *this;
    }

    ~Socket() noexcept
    {
        close();
    }

    [[nodiscard]] static Socket Adopt(std::uintptr_t handle) noexcept;

    std::uintptr_t native_handle() const noexcept
    {
        return handle_;
    }

    void swap(Socket &other) noexcept
    {
        std::swap(handle_, other.handle_);
    }

    bool is_valid() const noexcept
    {
        return handle_ != kInvalidHandle;
    }

    void bind(const Address &local);
    void listen(int backlog = 4096);

    AcceptResult accept();
    ReceiveResult receive(std::span<char> buffer);
    SendResult send(std::span<const char> buffer);

    void connect(const Address &peer);

    void set_non_blocking(bool enable = true);
    void set_reuse_address(bool enable = true);
    void set_reuse_port(bool enable = true);
    void set_keep_alive(bool enable = true);

    void get_local_address(Address &out) const;
    void get_peer_address(Address &out) const;

    void shutdown(ShutdownHow how = ShutdownHow::kBoth) noexcept;
    void close() noexcept;

  private:
    template <typename T> void set_native_option(int level, int option, const T &value);
    static std::error_code get_last_error() noexcept;

    constexpr static std::uintptr_t kInvalidHandle{ static_cast<std::uintptr_t>(-1) };
    std::uintptr_t handle_ = kInvalidHandle;

    bool non_blocking_ : 1 {false};
    bool reuse_address_ : 1 {false};
    bool reuse_port_ : 1 {false};
    bool keep_alive_ : 1 {false};
};

struct AcceptResult
{
    AcceptStatus status{AcceptStatus::kPending};
    Socket socket;
    Address address;
    std::error_code error_code{};
};
} // namespace Foundation::Core
