#pragma once

#include <cstddef>
#include <span>
#include <system_error>

namespace nbio::fs {
enum class OperationStatus {
    kDone,
    kPending,
    kError,
};

struct Transmission {
    OperationStatus status{OperationStatus::kPending};
    std::span<char> buffer;
    std::size_t bytes{0};
    std::error_code error_code{};
};
}  // namespace nbio::fs