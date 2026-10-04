#pragma once

#include <NBIO/Utility/Expected.hpp>
#include <NBIO/Net/SocketAddress.hpp>
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

namespace NBIO::Net {
class TcpSocket {
   public:
    enum class Type {
        kStream,
        kDatagram,
    };
    enum class ShutdownHow { kRead, kWrite, kBoth };

    TcpSocket() noexcept = default;
    TcpSocket(SocketAddress::Family family, Type type);
    TcpSocket(const TcpSocket&) = delete;
    TcpSocket& operator=(const TcpSocket&) = delete;
    TcpSocket(TcpSocket&& other) noexcept { std::swap(handle_, other.handle_); }
    TcpSocket& operator=(TcpSocket&& other) noexcept {
        if (this != &other) {
            close();
            handle_ = std::exchange(other.handle_, kInvalidHandle);
        }
        return *this;
    }
    ~TcpSocket() noexcept { close(); }

    [[nodiscard]] static TcpSocket adopt(std::uintptr_t handle) noexcept;

    std::uintptr_t native_handle() const noexcept { return handle_; }
    void swap(TcpSocket& other) noexcept { std::swap(handle_, other.handle_); }
    bool is_valid() const noexcept { return handle_ != kInvalidHandle; }

    Utility::expected<void, std::error_code> bind(const SocketAddress& local) noexcept;
    Utility::expected<void, std::error_code> listen(int backlog = 4096) noexcept;
    Utility::expected<std::pair<TcpSocket, SocketAddress>, std::error_code> accept() noexcept;
    Utility::expected<std::size_t, std::error_code> receive(std::span<char> buffer) noexcept;
    Utility::expected<std::size_t, std::error_code> send(std::span<const char> buffer) noexcept;
    Utility::expected<void, std::error_code> connect(const SocketAddress& peer) noexcept;

    Utility::expected<void, std::error_code> non_blocking(bool toggle = true) noexcept;
    Utility::expected<void, std::error_code> take_error() const noexcept;
    Utility::expected<void, std::error_code> reuse_address(bool toggle = true) noexcept;
    Utility::expected<void, std::error_code> reuse_port(bool toggle = true) noexcept;
    Utility::expected<void, std::error_code> keep_alive(bool toggle = true) noexcept;

    Utility::expected<SocketAddress, std::error_code> local_address() const noexcept;
    Utility::expected<SocketAddress, std::error_code> peer_address() const noexcept;

    void shutdown(ShutdownHow how = ShutdownHow::kBoth) noexcept;
    void close() noexcept;

   private:
    template <typename T>
    std::error_code set_native_option(int level, int option, const T& value) noexcept;
    static std::error_code last_error() noexcept;

    constexpr static std::uintptr_t kInvalidHandle{static_cast<std::uintptr_t>(-1)};
    std::uintptr_t handle_ = kInvalidHandle;
};
}  // namespace NBIO::Net
