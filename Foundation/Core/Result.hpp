#pragma once
#include <cstddef>
#include <system_error>
#include "Socket.hpp"
#include "Address.hpp"

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
    std::size_t bytes_transferred{0};
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
    std::size_t bytes_transferred{0};
    std::error_code error_code{};
};

enum class AcceptStatus
{
    kDone,
    kPending,
    kPeerClosed,
    kError,
};

struct AcceptResult
{
    AcceptStatus status{AcceptStatus::kPending};
    Socket socket;
    Address address;
    std::error_code error_code{};
};

} // namespace Foundation::Core