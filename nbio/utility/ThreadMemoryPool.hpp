#pragma once

#include <cstddef>

namespace nbio::utility {
struct ThreadMemoryPoolMetrics {
    std::size_t cached_chunks{0};
    std::size_t cached_bytes{0};
};

ThreadMemoryPoolMetrics thread_memory_pool_metrics() noexcept;
}  // namespace nbio::core


