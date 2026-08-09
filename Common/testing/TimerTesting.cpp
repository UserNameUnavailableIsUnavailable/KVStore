#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <deque>
#include <string>
#include <vector>

#include <poll.h>

#include "Common/Message.hpp"
#include "Common/Task.hpp"
#include "Common/Timer.hpp"
#include "Common/TimerQueue.hpp"

namespace
{
using namespace std::chrono_literals;
using Clock = KV::TimerQueue::Clock;

bool WaitReadable(int handle, std::chrono::milliseconds timeout)
{
    ::pollfd descriptor {.fd = handle, .events = POLLIN, .revents = 0};
    return ::poll(&descriptor, 1, static_cast<int>(timeout.count())) == 1;
}

// A stand-in for the real backends that keeps their structure: one TimerQueue,
// one ready deque, and a Wait() that blocks on the single timer fd.  Only the
// timer half of the interface is exercised, which is the surface under test.
class StubMessageQueue final : public KV::MessageQueue
{
public:
    void RegisterRead(KV::ReceiveOperation&) override {}
    void RegisterWrite(KV::SendOperation&) override {}
    bool RegisterConnect(KV::ConnectOperation&) override { return false; }
    void CancelConnect(KV::ConnectOperation&) noexcept override {}

    void RegisterTimer(KV::DelayedOperation& task) override
    {
        task.SetToken(timers_.Schedule(task.GetDeadline(), &task));
        ++registrations_;
    }

    void CancelTimer(KV::DelayedOperation& task) noexcept override
    {
        if (timers_.Cancel(task.GetToken()))
        {
            ++cancellations_;
        }
    }

    KV::Message Wait() override
    {
        while (true)
        {
            if (!ready_.empty())
            {
                const KV::Message message = ready_.front();
                ready_.pop_front();
                return message;
            }
            if (timers_.Empty())
            {
                return {.type = KV::MessageType::kTimer,
                    .session_id = KV::Message::kNoSession,
                    .result = 0,
                    .continuation = {}};
            }
            if (!WaitReadable(timers_.GetNativeHandle(), 5s))
            {
                return {.type = KV::MessageType::kTimer,
                    .session_id = KV::Message::kNoSession,
                    .result = 0,
                    .continuation = {}};
            }
            CollectExpired();
        }
    }

    // Drive the loop until nothing is scheduled, resuming whoever wakes up.
    void RunUntilIdle()
    {
        while (!timers_.Empty() || !ready_.empty())
        {
            const KV::Message message = Wait();
            if (!message.continuation)
            {
                break;
            }
            message.continuation.resume();
        }
    }

    // Collect one batch without resuming, so a test can observe how many
    // coroutines a single expiration released.
    std::size_t CollectExpired()
    {
        expired_.clear();
        timers_.DrainExpired(expired_);
        for (KV::DelayedOperation* operation : expired_)
        {
            operation->Complete(true);
            ready_.push_back({.type = KV::MessageType::kTimer,
                .session_id = KV::Message::kNoSession,
                .result = 0,
                .continuation = operation->GetContinuation()});
        }
        return expired_.size();
    }

    void ResumeReady()
    {
        while (!ready_.empty())
        {
            const KV::Message message = ready_.front();
            ready_.pop_front();
            if (message.continuation)
            {
                message.continuation.resume();
            }
        }
    }

    bool HasPending() const noexcept { return !timers_.Empty(); }
    std::size_t GetPendingCount() const noexcept { return timers_.Size(); }
    std::size_t GetRegistrationCount() const noexcept { return registrations_; }
    std::size_t GetCancellationCount() const noexcept { return cancellations_; }
    KV::TimerQueue& GetTimerQueue() noexcept { return timers_; }

private:
    KV::TimerQueue timers_;
    std::deque<KV::Message> ready_;
    std::vector<KV::DelayedOperation*> expired_;
    std::size_t registrations_ = 0;
    std::size_t cancellations_ = 0;
};

KV::SessionTask WaitOnce(KV::MessageQueue& queue, std::chrono::nanoseconds delay,
    bool& elapsed, bool& finished)
{
    elapsed = co_await KV::DelayedOperation(queue, delay);
    finished = true;
}

// Records the order in which concurrent waits complete.
KV::SessionTask WaitAndRecord(KV::MessageQueue& queue, std::chrono::nanoseconds delay,
    std::string name, std::vector<std::string>& order)
{
    co_await KV::DelayedOperation(queue, delay);
    order.push_back(std::move(name));
}

// Abandons its wait: the frame is destroyed while the timer is still scheduled,
// which is what makes the self-withdrawal matter.
KV::SessionTask WaitForever(KV::MessageQueue& queue, bool& resumed)
{
    co_await KV::DelayedOperation(queue, 1h);
    resumed = true;
}

// Schedules a second wait from inside the first one's resumption, which mutates
// the heap while the queue is still draining a batch.
KV::SessionTask WaitTwice(KV::MessageQueue& queue, int& stage)
{
    co_await KV::DelayedOperation(queue, 5ms);
    stage = 1;
    co_await KV::DelayedOperation(queue, 5ms);
    stage = 2;
}
} // namespace

// =============================================================================
// Timer: the timerfd wrapper
// =============================================================================

TEST(TimerTesting, StartsDisarmed)
{
    KV::Timer timer;

    EXPECT_TRUE(timer.IsOpen());
    EXPECT_GE(timer.GetNativeHandle(), 0);
    EXPECT_FALSE(timer.IsArmed());
    EXPECT_EQ(timer.Drain(), 0u); // nothing pending, and no blocking
}

TEST(TimerTesting, ArmsOnConstructionAndFires)
{
    KV::Timer timer(20ms);

    EXPECT_TRUE(timer.IsArmed());
    ASSERT_TRUE(WaitReadable(timer.GetNativeHandle(), 2s));
    EXPECT_EQ(timer.Drain(), 1u);
}

TEST(TimerTesting, DoesNotFireBeforeItsDeadline)
{
    KV::Timer timer(500ms);

    EXPECT_FALSE(WaitReadable(timer.GetNativeHandle(), 20ms));
    EXPECT_EQ(timer.Drain(), 0u);
}

TEST(TimerTesting, DrainConsumesTheExpirationCount)
{
    KV::Timer timer(10ms);
    ASSERT_TRUE(WaitReadable(timer.GetNativeHandle(), 2s));

    EXPECT_EQ(timer.Drain(), 1u);
    // A one-shot timer stays quiet once drained, so a level-triggered poller
    // will not spin on it.
    EXPECT_FALSE(WaitReadable(timer.GetNativeHandle(), 20ms));
    EXPECT_EQ(timer.Drain(), 0u);
}

TEST(TimerTesting, AZeroTimeoutStillFires)
{
    // timerfd reads an all-zero it_value as "disarm", so without clamping this
    // timer would never fire and an awaiting coroutine would hang forever.
    KV::Timer timer(0ms);

    ASSERT_TRUE(WaitReadable(timer.GetNativeHandle(), 2s));
    EXPECT_GE(timer.Drain(), 1u);
}

TEST(TimerTesting, ANegativeTimeoutStillFires)
{
    KV::Timer timer(-50ms);

    ASSERT_TRUE(WaitReadable(timer.GetNativeHandle(), 2s));
    EXPECT_GE(timer.Drain(), 1u);
}

TEST(TimerTesting, AcceptsAnyDurationUnit)
{
    KV::Timer nanoseconds(std::chrono::nanoseconds(1));
    KV::Timer microseconds(500us);
    KV::Timer milliseconds(5ms);

    EXPECT_TRUE(WaitReadable(nanoseconds.GetNativeHandle(), 2s));
    EXPECT_TRUE(WaitReadable(microseconds.GetNativeHandle(), 2s));
    EXPECT_TRUE(WaitReadable(milliseconds.GetNativeHandle(), 2s));
}

TEST(TimerTesting, SubSecondAndMultiSecondDelaysSplitCorrectly)
{
    // Guards the seconds / nanoseconds split in ToTimespec: tv_nsec must stay
    // below one second or timerfd_settime rejects it with EINVAL.
    KV::Timer timer;
    EXPECT_NO_THROW(timer.SetTimeout(1500ms));
    EXPECT_TRUE(timer.IsArmed());
    EXPECT_NO_THROW(timer.SetTimeout(2s));
    EXPECT_TRUE(timer.IsArmed());
}

TEST(TimerTesting, AbsoluteDeadlineFires)
{
    KV::Timer timer;
    timer.SetDeadline(Clock::now() + 20ms);

    EXPECT_TRUE(timer.IsArmed());
    ASSERT_TRUE(WaitReadable(timer.GetNativeHandle(), 2s));
    EXPECT_EQ(timer.Drain(), 1u);
}

TEST(TimerTesting, ADeadlineAlreadyInThePastFiresImmediately)
{
    KV::Timer timer;
    timer.SetDeadline(Clock::now() - 1s);

    ASSERT_TRUE(WaitReadable(timer.GetNativeHandle(), 2s));
    EXPECT_GE(timer.Drain(), 1u);
}

TEST(TimerTesting, IntervalFiresRepeatedly)
{
    KV::Timer timer;
    timer.SetInterval(10ms);

    for (int tick = 0; tick < 3; ++tick)
    {
        ASSERT_TRUE(WaitReadable(timer.GetNativeHandle(), 2s)) << "tick " << tick;
        EXPECT_GE(timer.Drain(), 1u);
    }
}

TEST(TimerTesting, DisarmStopsAPendingTimer)
{
    KV::Timer timer(30ms);
    ASSERT_TRUE(timer.IsArmed());

    timer.Disarm();

    EXPECT_FALSE(timer.IsArmed());
    EXPECT_FALSE(WaitReadable(timer.GetNativeHandle(), 80ms));
}

TEST(TimerTesting, RearmingReplacesTheDeadline)
{
    KV::Timer timer(1h);
    timer.SetTimeout(10ms);

    ASSERT_TRUE(WaitReadable(timer.GetNativeHandle(), 2s));
    EXPECT_EQ(timer.Drain(), 1u);
}

TEST(TimerTesting, MoveTransfersTheDescriptor)
{
    KV::Timer source(10ms);
    const KV::Timer::HandleType handle = source.GetNativeHandle();

    KV::Timer moved(std::move(source));

    EXPECT_EQ(moved.GetNativeHandle(), handle);
    EXPECT_FALSE(source.IsOpen());
    ASSERT_TRUE(WaitReadable(moved.GetNativeHandle(), 2s));
    EXPECT_EQ(moved.Drain(), 1u);
}

// =============================================================================
// TimerQueue: the indexed min-heap
// =============================================================================

TEST(TimerQueueTesting, StartsEmptyWithOneDescriptor)
{
    KV::TimerQueue queue;

    EXPECT_TRUE(queue.Empty());
    EXPECT_EQ(queue.Size(), 0u);
    EXPECT_GE(queue.GetNativeHandle(), 0);
    EXPECT_FALSE(queue.GetEarliestDeadline().has_value());
}

TEST(TimerQueueTesting, TheEarliestDeadlineSurfacesRegardlessOfInsertionOrder)
{
    KV::TimerQueue queue;
    const Clock::time_point base = Clock::now();

    // Deliberately out of order: the heap, not the caller, decides the head.
    queue.Schedule(base + 400ms, nullptr);
    queue.Schedule(base + 100ms, nullptr);
    queue.Schedule(base + 900ms, nullptr);
    queue.Schedule(base + 200ms, nullptr);

    ASSERT_TRUE(queue.GetEarliestDeadline().has_value());
    EXPECT_EQ(*queue.GetEarliestDeadline(), base + 100ms);
    EXPECT_EQ(queue.Size(), 4u);
}

TEST(TimerQueueTesting, CancellingTheHeadPromotesTheNextDeadline)
{
    KV::TimerQueue queue;
    const Clock::time_point base = Clock::now();

    const KV::TimerQueue::Token first = queue.Schedule(base + 100ms, nullptr);
    queue.Schedule(base + 200ms, nullptr);

    ASSERT_TRUE(queue.Cancel(first));

    ASSERT_TRUE(queue.GetEarliestDeadline().has_value());
    EXPECT_EQ(*queue.GetEarliestDeadline(), base + 200ms);
    EXPECT_EQ(queue.Size(), 1u);
}

TEST(TimerQueueTesting, CancellingAnInteriorEntryKeepsTheHeapOrdered)
{
    KV::TimerQueue queue;
    const Clock::time_point base = Clock::now();

    queue.Schedule(base + 100ms, nullptr);
    const KV::TimerQueue::Token middle = queue.Schedule(base + 300ms, nullptr);
    queue.Schedule(base + 500ms, nullptr);
    queue.Schedule(base + 700ms, nullptr);

    // Removing from the middle is exactly what a plain priority_queue cannot do.
    ASSERT_TRUE(queue.Cancel(middle));

    EXPECT_EQ(queue.Size(), 3u);
    EXPECT_EQ(*queue.GetEarliestDeadline(), base + 100ms);
    EXPECT_FALSE(queue.Contains(middle));
}

TEST(TimerQueueTesting, CancellingTwiceIsHarmless)
{
    KV::TimerQueue queue;
    const KV::TimerQueue::Token token = queue.Schedule(Clock::now() + 1h, nullptr);

    EXPECT_TRUE(queue.Cancel(token));
    // The caller often cannot know whether a timer fired first, so a second
    // cancellation reports false instead of corrupting the heap.
    EXPECT_FALSE(queue.Cancel(token));
    EXPECT_TRUE(queue.Empty());
}

TEST(TimerQueueTesting, ADefaultTokenNeverResolves)
{
    KV::TimerQueue queue;
    queue.Schedule(Clock::now() + 1h, nullptr);

    const KV::TimerQueue::Token empty;
    EXPECT_FALSE(empty.IsValid());
    EXPECT_FALSE(queue.Contains(empty));
    EXPECT_FALSE(queue.Cancel(empty));
    EXPECT_EQ(queue.Size(), 1u);
}

TEST(TimerQueueTesting, AStaleTokenDoesNotAddressTheSlotsNewTimer)
{
    KV::TimerQueue queue;

    const KV::TimerQueue::Token stale = queue.Schedule(Clock::now() + 1h, nullptr);
    ASSERT_TRUE(queue.Cancel(stale));

    // The freed slot is handed straight back out, so this reuses stale's index.
    const KV::TimerQueue::Token fresh = queue.Schedule(Clock::now() + 1h, nullptr);
    ASSERT_EQ(fresh.index, stale.index);
    ASSERT_NE(fresh.generation, stale.generation);

    EXPECT_FALSE(queue.Contains(stale));
    EXPECT_FALSE(queue.Cancel(stale)); // must not evict the new timer
    EXPECT_TRUE(queue.Contains(fresh));
    EXPECT_EQ(queue.Size(), 1u);
}

TEST(TimerQueueTesting, MaintainsHeapOrderUnderRandomizedChurn)
{
    KV::TimerQueue queue;
    const Clock::time_point base = Clock::now();
    std::vector<std::pair<Clock::time_point, KV::TimerQueue::Token>> live;

    // A scattered insertion pattern exercises both sift directions.
    for (int step = 0; step < 200; ++step)
    {
        const auto offset = std::chrono::milliseconds((step * 37) % 101 + 1);
        live.emplace_back(base + offset, queue.Schedule(base + offset, nullptr));
    }
    // Cancel a third of them, hitting arbitrary interior positions.
    for (std::size_t index = 0; index < live.size(); index += 3)
    {
        ASSERT_TRUE(queue.Cancel(live[index].second));
        live[index].second = {};
    }
    std::erase_if(live, [](const auto& entry) { return !entry.second.IsValid(); });

    ASSERT_EQ(queue.Size(), live.size());
    const auto smallest = std::ranges::min_element(live, [](const auto& left, const auto& right) {
        return left.first < right.first;
    });
    ASSERT_TRUE(queue.GetEarliestDeadline().has_value());
    EXPECT_EQ(*queue.GetEarliestDeadline(), smallest->first);
}

TEST(TimerQueueTesting, DrainCollectsOnlyExpiredDeadlines)
{
    KV::TimerQueue queue;
    const Clock::time_point past = Clock::now() - 1s;

    queue.Schedule(past, nullptr);
    queue.Schedule(past, nullptr);
    queue.Schedule(Clock::now() + 1h, nullptr);

    std::vector<KV::DelayedOperation*> expired;
    queue.DrainExpired(expired);

    EXPECT_EQ(expired.size(), 2u);
    EXPECT_EQ(queue.Size(), 1u); // the far-future timer stays scheduled
}

TEST(TimerQueueTesting, DrainOnAnEmptyQueueIsANoOp)
{
    KV::TimerQueue queue;
    std::vector<KV::DelayedOperation*> expired;

    queue.DrainExpired(expired);

    EXPECT_TRUE(expired.empty());
    EXPECT_TRUE(queue.Empty());
}

// =============================================================================
// DelayedOperation: the awaiter
// =============================================================================

TEST(DelayedOperationTesting, AlwaysSuspends)
{
    StubMessageQueue queue;
    const KV::DelayedOperation operation(queue, 0ms);

    // Even a zero delay suspends: a delay that returned inline would not be a
    // delay, and the message loop would never get a chance to run.
    EXPECT_FALSE(operation.await_ready());
}

TEST(DelayedOperationTesting, TheDeadlineIsFixedWhenTheOperationIsCreated)
{
    StubMessageQueue queue;
    const Clock::time_point before = Clock::now();
    const KV::DelayedOperation operation(queue, 50ms);
    const Clock::time_point after = Clock::now();

    EXPECT_GE(operation.GetDeadline(), before + 50ms);
    EXPECT_LE(operation.GetDeadline(), after + 50ms);
}

TEST(DelayedOperationTesting, SchedulesOnSuspensionAndResumesOnExpiry)
{
    StubMessageQueue queue;
    bool elapsed = false;
    bool finished = false;

    KV::SessionTask task = WaitOnce(queue, 10ms, elapsed, finished);

    // Parked inside the co_await, holding one heap slot.
    EXPECT_FALSE(finished);
    EXPECT_EQ(queue.GetPendingCount(), 1u);

    queue.RunUntilIdle();

    EXPECT_TRUE(finished);
    EXPECT_TRUE(elapsed);
    EXPECT_TRUE(task.Done());
    EXPECT_FALSE(queue.HasPending());
}

TEST(DelayedOperationTesting, ManyConcurrentWaitsShareOneDescriptor)
{
    StubMessageQueue queue;
    std::vector<std::string> order;
    std::vector<KV::SessionTask> tasks;

    // Registered latest-first so the heap has to reorder them.
    tasks.push_back(WaitAndRecord(queue, 45ms, "third", order));
    tasks.push_back(WaitAndRecord(queue, 15ms, "first", order));
    tasks.push_back(WaitAndRecord(queue, 30ms, "second", order));

    EXPECT_EQ(queue.GetPendingCount(), 3u);

    queue.RunUntilIdle();

    // Deadline order, not registration order.
    ASSERT_EQ(order.size(), 3u);
    EXPECT_EQ(order[0], "first");
    EXPECT_EQ(order[1], "second");
    EXPECT_EQ(order[2], "third");
}

TEST(DelayedOperationTesting, OneExpirationCanReleaseAWholeBatch)
{
    StubMessageQueue queue;
    std::vector<std::string> order;
    std::vector<KV::SessionTask> tasks;

    // All three share a deadline that has already passed by the time the loop
    // looks, so a single fd readiness must release all of them.
    for (int index = 0; index < 3; ++index)
    {
        tasks.push_back(WaitAndRecord(queue, 5ms, std::to_string(index), order));
    }
    ASSERT_EQ(queue.GetPendingCount(), 3u);
    ASSERT_TRUE(WaitReadable(queue.GetTimerQueue().GetNativeHandle(), 2s));

    const std::size_t collected = queue.CollectExpired();

    EXPECT_EQ(collected, 3u);
    EXPECT_FALSE(queue.HasPending());

    queue.ResumeReady();
    EXPECT_EQ(order.size(), 3u);
}

TEST(DelayedOperationTesting, ReportsWhetherTheDelayActuallyElapsed)
{
    StubMessageQueue queue;
    KV::DelayedOperation operation(queue, 1h);

    // Never completed: reporting "elapsed" here would let a coroutine mistake a
    // truncated wait for a real one.
    EXPECT_FALSE(operation.await_resume());

    operation.Complete(true);
    EXPECT_TRUE(operation.await_resume());

    // A cut-short wait is an expected outcome, reported rather than thrown.
    operation.Complete(false);
    EXPECT_FALSE(operation.await_resume());
}

TEST(DelayedOperationTesting, DestroyingAPendingWaitWithdrawsItFromTheHeap)
{
    StubMessageQueue queue;
    bool resumed = false;

    {
        KV::SessionTask task = WaitForever(queue, resumed);
        ASSERT_EQ(queue.GetPendingCount(), 1u);
        ASSERT_EQ(queue.GetCancellationCount(), 0u);
    } // the frame is destroyed here, taking the DelayedOperation with it

    // Without the destructor hook the heap would still hold a pointer into the
    // freed frame, and the next drain would resume a dangling continuation.
    EXPECT_EQ(queue.GetCancellationCount(), 1u);
    EXPECT_FALSE(queue.HasPending());
    EXPECT_FALSE(resumed);
}

TEST(DelayedOperationTesting, ACompletedWaitIsNotCancelledAgain)
{
    StubMessageQueue queue;
    bool elapsed = false;
    bool finished = false;

    {
        KV::SessionTask task = WaitOnce(queue, 10ms, elapsed, finished);
        queue.RunUntilIdle();
        ASSERT_TRUE(finished);
    }

    // Complete() clears the token, so teardown must not withdraw a heap slot
    // that has already been released and possibly handed to another timer.
    EXPECT_EQ(queue.GetCancellationCount(), 0u);
}

TEST(DelayedOperationTesting, AbandoningOneWaitLeavesTheOthersScheduled)
{
    StubMessageQueue queue;
    std::vector<std::string> order;
    bool resumed = false;

    KV::SessionTask survivor = WaitAndRecord(queue, 15ms, "survivor", order);
    {
        KV::SessionTask abandoned = WaitForever(queue, resumed);
        ASSERT_EQ(queue.GetPendingCount(), 2u);
    }

    EXPECT_EQ(queue.GetPendingCount(), 1u);
    queue.RunUntilIdle();

    ASSERT_EQ(order.size(), 1u);
    EXPECT_EQ(order[0], "survivor");
    EXPECT_FALSE(resumed);
}

TEST(DelayedOperationTesting, ResumingMayScheduleAFurtherWait)
{
    StubMessageQueue queue;
    int stage = 0;

    // The second co_await runs inside the first one's resumption, mutating the
    // heap while the loop is still working through a drained batch.
    KV::SessionTask task = WaitTwice(queue, stage);
    queue.RunUntilIdle();

    EXPECT_EQ(stage, 2);
    EXPECT_TRUE(task.Done());
    EXPECT_FALSE(queue.HasPending());
}

TEST(DelayedOperationTesting, HeapSlotsAreReusedAcrossSuccessiveWaits)
{
    StubMessageQueue queue;

    for (int round = 0; round < 5; ++round)
    {
        bool elapsed = false;
        bool finished = false;
        KV::SessionTask task = WaitOnce(queue, 5ms, elapsed, finished);
        queue.RunUntilIdle();
        ASSERT_TRUE(finished) << "round " << round;
        ASSERT_TRUE(elapsed) << "round " << round;
    }

    EXPECT_EQ(queue.GetRegistrationCount(), 5u);
    EXPECT_FALSE(queue.HasPending());
}

// =============================================================================
// setTimeout / setInterval: detached scheduling
// =============================================================================

TEST(SetTimeoutTesting, RunsTheCallbackOnce)
{
    StubMessageQueue queue;
    int calls = 0;

    KV::SetTimeout(queue, 10ms, [&calls] { ++calls; });

    // The callback has not run yet: the detached task is parked on its deadline.
    EXPECT_EQ(calls, 0);
    EXPECT_TRUE(queue.HasPending());

    queue.RunUntilIdle();

    EXPECT_EQ(calls, 1);
    EXPECT_FALSE(queue.HasPending());
}

TEST(SetTimeoutTesting, TheCallbackOutlivesTheCallersScope)
{
    StubMessageQueue queue;
    std::string observed;

    {
        // The captured value lives in the coroutine frame, which the detached
        // task owns; nothing here has to stay alive for the callback to work.
        const std::string message = "fired";
        KV::SetTimeout(queue, 5ms, [&observed, message] { observed = message; });
    }

    queue.RunUntilIdle();

    EXPECT_EQ(observed, "fired");
}

TEST(SetTimeoutTesting, SeveralTimeoutsFireInDeadlineOrder)
{
    StubMessageQueue queue;
    std::vector<int> order;

    KV::SetTimeout(queue, 30ms, [&order] { order.push_back(3); });
    KV::SetTimeout(queue, 10ms, [&order] { order.push_back(1); });
    KV::SetTimeout(queue, 20ms, [&order] { order.push_back(2); });

    EXPECT_EQ(queue.GetPendingCount(), 3u);
    queue.RunUntilIdle();

    ASSERT_EQ(order.size(), 3u);
    EXPECT_EQ(order[0], 1);
    EXPECT_EQ(order[1], 2);
    EXPECT_EQ(order[2], 3);
}

TEST(SetIntervalTesting, RepeatsUntilTheCallbackDeclines)
{
    StubMessageQueue queue;
    int ticks = 0;

    KV::SetInterval(queue, 5ms, [&ticks] {
        ++ticks;
        return ticks < 3;
    });

    queue.RunUntilIdle();

    EXPECT_EQ(ticks, 3);
    EXPECT_FALSE(queue.HasPending());
    // Three ticks, three waits: the interval re-arms once per surviving tick.
    EXPECT_EQ(queue.GetRegistrationCount(), 3u);
}

TEST(SetIntervalTesting, StopsImmediatelyWhenTheFirstTickDeclines)
{
    StubMessageQueue queue;
    int ticks = 0;

    KV::SetInterval(queue, 5ms, [&ticks] {
        ++ticks;
        return false;
    });

    queue.RunUntilIdle();

    EXPECT_EQ(ticks, 1);
    EXPECT_EQ(queue.GetRegistrationCount(), 1u);
}

TEST(SetIntervalTesting, TwoIntervalsInterleaveByDeadline)
{
    StubMessageQueue queue;
    int fast = 0;
    int slow = 0;

    KV::SetInterval(queue, 5ms, [&fast] {
        ++fast;
        return fast < 4;
    });
    KV::SetInterval(queue, 12ms, [&slow] {
        ++slow;
        return slow < 2;
    });

    queue.RunUntilIdle();

    EXPECT_EQ(fast, 4);
    EXPECT_EQ(slow, 2);
    EXPECT_FALSE(queue.HasPending());
}
