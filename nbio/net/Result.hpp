#pragma once

#include <nbio/net/Address.hpp>
#include <nbio/net/TcpSocket.hpp>
#include <cstddef>
#include <span>
#include <system_error>

namespace nbio::net {
enum class OperationStatus {
    kDone,
    kPending,
    kError,
};

struct Communication {
    OperationStatus status{};
    TcpSocket socket{};
    Address address{};
    std::error_code error_code{};
};

struct Transmission {
    OperationStatus status{OperationStatus::kPending};
    std::span<char> buffer;
    std::size_t bytes{0};
    std::error_code error_code{};
};
}  // namespace nbio::net