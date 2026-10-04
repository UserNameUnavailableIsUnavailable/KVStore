#pragma once

#include <NBIO/Net/SocketAddress.hpp>
#include <NBIO/Net/TcpSocket.hpp>
#include <cstddef>
#include <span>
#include <system_error>

namespace NBIO::Net {
enum class OperationStatus {
    kDone,
    kPending,
    kError,
};

struct Communication {
    OperationStatus status{};
    TcpSocket socket{};
    SocketAddress address{};
    std::error_code error_code{};
};

struct Transmission {
    OperationStatus status{OperationStatus::kPending};
    std::span<char> buffer;
    std::size_t bytes{0};
    std::error_code error_code{};
};
}  // namespace NBIO::Net