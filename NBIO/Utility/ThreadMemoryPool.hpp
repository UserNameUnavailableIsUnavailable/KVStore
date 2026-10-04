#pragma once

#include <cstddef>

namespace NBIO::Utility {
struct ThreadMemoryPoolMetrics {
    std::size_t cached_chunks{0};
    std::size_t cached_bytes{0};
};

ThreadMemoryPoolMetrics thread_memory_pool_metrics() noexcept;
}  // namespace NBIO::Core


