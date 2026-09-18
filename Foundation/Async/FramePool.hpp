#pragma once

#include <chrono>
#include <cstddef>

namespace Foundation::Async
{
// How long one epoch lasts: how often the pool reconsiders each class's
// prediction. It is the single number that decides how fast the pool follows a
// workload -- a demand that appears and disappears inside one epoch never moves
// the prediction at all -- so it is here rather than buried in the
// implementation, and a test that waits for an epoch can wait this long.
inline constexpr std::chrono::milliseconds kFramePoolEpoch{15000};
// Coroutine-frame allocator used by Promise::operator new / delete. The many
// small frames an await chain creates per request are allocated and destroyed on
// one scheduler thread, so each thread keeps its own size-classed free list:
// reusing a chunk avoids the malloc/free churn of repeatedly constructing and
// destroying those frames.
//
// The list is adaptive rather than simply unbounded. Once an epoch, each class
// compares the frames it actually had in use with what it predicted, and moves
// its prediction halfway to the observation -- never below it, so demand that
// has just arrived is served next time. The cache then holds a quarter more than
// the prediction, so a burst has somewhere to land; the pool as a whole holds at
// most a few MiB back from the allocator; and frames larger than the largest
// class fall through to the global allocator untouched.
void *frame_allocate(std::size_t size);
void frame_deallocate(void *pointer, std::size_t size) noexcept;

// What the pool is holding back from the allocator right now. A cache that
// cannot be observed is a leak that is harder to see than a plain one, so this
// is here to be watched.
struct FramePoolMetrics
{
    std::size_t live_frames{0};   // frames checked out by coroutines
    std::size_t cached_chunks{0}; // frames kept for the next request
    std::size_t cached_bytes{0};  // the memory those chunks hold
};

FramePoolMetrics frame_pool_metrics() noexcept;
} // namespace Foundation::Async
