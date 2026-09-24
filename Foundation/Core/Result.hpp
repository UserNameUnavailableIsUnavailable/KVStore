#pragma once

#include <cstddef>
#include <system_error>

#include "TcpSocket.hpp"

namespace Foundation::Core
{
enum class OperationStatus
{
    kDone,
    kPending,
    kError,
};

struct Communication
{
    OperationStatus status{};
    TcpSocket socket{}; // CAVEAT: this does not imply this buffer is mutable! Non-const is for C API compatibility.
    SocketAddress address{}; // CAVEAT: this does not imply this buffer is mutable! Non-const is for C API compatibility.
    std::error_code error_code{};
};

struct Transmission
{
    OperationStatus status{OperationStatus::kPending};
    std::span<char> buffer; // CAVEAT: this does not imply this buffer is mutable! Non-const is for C API compatibility.
    std::size_t bytes{0};
    std::error_code error_code{};
};

} // namespace Foundation::Core