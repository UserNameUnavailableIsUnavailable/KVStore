#pragma once

#include <nbio/utility/Expected.hpp>
#include <nbio/net/Address.hpp>
#include <span>
#include <system_error>
#include <utility>

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

namespace nbio::net {
class TcpSocket {
   public:
    enum class Type {
        kStream,
        kDatagram,
    };
    enum class ShutdownHow { kRead, kWrite, kBoth };

    TcpSocket() noexcept = default;
    TcpSocket(Address::Family family, Type type);
    TcpSocket(const TcpSocket&) = delete;
    TcpSocket& operator=(const TcpSocket&) = delete;
    TcpSocket(TcpSocket&& other) noexcept { std::swap(handle_, other.handle_); }
    TcpSocket& operator=(TcpSocket&& other) noexcept {
        if (this != &other) {
            Close();
            handle_ = std::exchange(other.handle_, kInvalidHandle);
        }
        return *this;
    }
    ~TcpSocket() noexcept { Close(); }

    [[nodiscard]] static TcpSocket Adopt(std::uintptr_t handle) noexcept;

    std::uintptr_t native_handle() const noexcept { return handle_; }
    void swap(TcpSocket& other) noexcept { std::swap(handle_, other.handle_); }
    bool IsValid() const noexcept { return handle_ != kInvalidHandle; }

    utility::expected<void, std::error_code> Bind(const Address& local) noexcept;
    utility::expected<void, std::error_code> Listen(int backlog = 4096) noexcept;
    utility::expected<std::pair<TcpSocket, Address>, std::error_code> Accept() noexcept;
    utility::expected<std::size_t, std::error_code> Receive(std::span<char> buffer) noexcept;
    utility::expected<std::size_t, std::error_code> Send(std::span<const char> buffer) noexcept;
    utility::expected<void, std::error_code> Connect(const Address& peer) noexcept;

    utility::expected<void, std::error_code> NonBlocking(bool toggle = true) noexcept;
    utility::expected<void, std::error_code> TakeError() const noexcept;
    utility::expected<void, std::error_code> ReuseAddress(bool toggle = true) noexcept;
    utility::expected<void, std::error_code> ReusePort(bool toggle = true) noexcept;
    utility::expected<void, std::error_code> KeepAlive(bool toggle = true) noexcept;

    utility::expected<Address, std::error_code> GetLocalAddress() const noexcept;
    utility::expected<Address, std::error_code> GetPeerAddress() const noexcept;

    void Shutdown(ShutdownHow how = ShutdownHow::kBoth) noexcept;
    void Close() noexcept;

   private:
    template <typename T>
    std::error_code SetNativeOption(int level, int option, const T& value) noexcept;
    static std::error_code last_error() noexcept;

    constexpr static std::uintptr_t kInvalidHandle{static_cast<std::uintptr_t>(-1)};
    std::uintptr_t handle_ = kInvalidHandle;
};
}  // namespace nbio::net
