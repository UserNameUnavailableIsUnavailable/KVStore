#pragma once

#include <chrono>
#include <cstddef>

namespace nbio::async {
inline constexpr std::chrono::milliseconds kFramePoolEpoch{1000};
void* frame_allocate(std::size_t size);
void frame_deallocate(void* pointer, std::size_t size) noexcept;

struct FramePoolMetrics {
    std::size_t live_frames{0};    // frames checked out by coroutines
    std::size_t cached_chunks{0};  // frames kept for the next request
    std::size_t cached_bytes{0};   // the memory those chunks hold
};

FramePoolMetrics frame_pool_metrics() noexcept;
}  // namespace nbio::async
