// The frame pool is a cache with a policy, so what it does has to be checked
// from both ends: what it keeps has to come back to the next request that wants
// it, and what it keeps has to stop somewhere.
//
// The pool is thread-local process state, so these tests share it. The one that
// fills the pool's budget to prove the bound runs last, because a saturated
// budget is exactly what the tests before it must not be working against.
#include <Foundation/Async/FramePool.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <thread>
#include <vector>

namespace
{
// Mirrors kRetainedBytesBudget in FramePool.cpp: the policy's numbers are the
// policy, so the test names the bound it expects rather than deriving one.
constexpr std::size_t kRetainedBytesBudget = 4U * 1024U * 1024U;
} // namespace

// A frame that was released is the frame the next request gets. Everything else
// the pool does is worth nothing if this is not true.
TEST(FramePoolTesting, AReleasedFrameIsWhatTheNextRequestGets)
{
    void *first = Foundation::Async::frame_allocate(128);
    ASSERT_NE(first, nullptr);
    Foundation::Async::frame_deallocate(first, 128);

    void *second = Foundation::Async::frame_allocate(128);
    EXPECT_EQ(second, first);
    Foundation::Async::frame_deallocate(second, 128);
}

// A workload that uses one frame at a time keeps exactly one frame: the cache
// absorbs the whole cycle instead of handing the frame back and re-asking.
TEST(FramePoolTesting, ASteadyWorkloadReusesTheSameFrame)
{
    void *chunk = Foundation::Async::frame_allocate(256);
    ASSERT_NE(chunk, nullptr);
    Foundation::Async::frame_deallocate(chunk, 256);

    for (int round = 0; round < 100; ++round)
    {
        void *again = Foundation::Async::frame_allocate(256);
        EXPECT_EQ(again, chunk) << "round " << round;
        Foundation::Async::frame_deallocate(again, 256);
    }
}

// The prediction is what the cache is sized by, so a burst the epoch has seen
// must leave the pool ready for the next one: closing the epoch raises the
// prediction to that burst, the target becomes a quarter more than it, and the
// frees that follow are kept against it. The first burst cannot be served from
// the cache at all -- nothing has been predicted yet -- which is what "one epoch
// behind" means here.
TEST(FramePoolTesting, ABurstSetsThePredictionTheNextBurstIsServedFrom)
{
    // The epoch is the unit the prediction moves in, so this test cannot ask for
    // less than one of them, and it will not sit here for a long one.
    if (Foundation::Async::kFramePoolEpoch > std::chrono::seconds(2))
    {
        GTEST_SKIP() << "one epoch is " << Foundation::Async::kFramePoolEpoch.count()
                     << " ms, which is longer than a test should wait";
    }

    constexpr std::size_t kChunk = 32U * 1024U; // a class no other test uses
    constexpr std::size_t kBurst = 64;

    std::vector<void *> frames;
    const auto burst = [&frames](std::size_t count, std::size_t chunk) {
        frames.clear();
        frames.reserve(count);
        for (std::size_t index = 0; index < count; ++index)
        {
            frames.push_back(Foundation::Async::frame_allocate(chunk));
        }
        for (void *frame : frames)
        {
            Foundation::Async::frame_deallocate(frame, chunk);
        }
    };

    burst(kBurst, kChunk);
    const std::size_t before = Foundation::Async::frame_pool_metrics().cached_bytes;

    // Wait out the epoch, then give it releases to act on: the epoch closes with
    // this burst as the observation.
    std::this_thread::sleep_for(Foundation::Async::kFramePoolEpoch + std::chrono::milliseconds(200));
    for (int round = 0; round < 1024; ++round)
    {
        void *frame = Foundation::Async::frame_allocate(kChunk);
        Foundation::Async::frame_deallocate(frame, kChunk);
    }

    burst(kBurst, kChunk);
    const std::size_t after = Foundation::Async::frame_pool_metrics().cached_bytes;

    EXPECT_GT(after, before) << "the burst the epoch saw did not change what is kept";
    EXPECT_GE(after - before, kBurst * kChunk / 2) << "the prediction is not being used to keep frames";
    EXPECT_LE(after, kRetainedBytesBudget) << "the pool held back more than it is allowed to";
}

// The bound is the part of the policy that keeps the pool honest: a class that
// was once used for far more frames than are in use now must not remember them
// all. Without the budget this test would have the pool keep half of 16 MiB.
TEST(FramePoolTesting, WhatIsCachedStaysWithinTheBudget)
{
    constexpr std::size_t kChunk = 64U * 1024U;
    constexpr std::size_t kFrames = 256; // 16 MiB in flight at once

    std::vector<void *> frames;
    frames.reserve(kFrames);
    for (std::size_t index = 0; index < kFrames; ++index)
    {
        frames.push_back(Foundation::Async::frame_allocate(kChunk));
    }
    for (void *frame : frames)
    {
        Foundation::Async::frame_deallocate(frame, kChunk);
    }

    const Foundation::Async::FramePoolMetrics metrics = Foundation::Async::frame_pool_metrics();
    EXPECT_GT(metrics.cached_bytes, 0U) << "nothing was kept for reuse at all";
    EXPECT_LE(metrics.cached_bytes, kRetainedBytesBudget) << "the pool held back more than it is allowed to";
    EXPECT_EQ(metrics.live_frames, 0U) << "every frame was released";
}
