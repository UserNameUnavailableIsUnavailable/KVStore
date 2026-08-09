#pragma once

#if not defined(__linux__)
#error "This header is linux-specific."
#endif

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <utility>
#include <vector>
#include "Common/Timer.hpp"

namespace KV
{
class DelayedOperation;

// TimerQueue schedules coroutine resumptions on a single timerfd.
//
// The naive alternative -- one timerfd per pending wait -- costs a descriptor
// per timer, which does not survive per-connection idle timeouts at 10k
// concurrency.  Here one fd serves every timer: the queue keeps deadlines in a
// min-heap and arms the fd only for the earliest one.  Scheduling a timer that
// is not the new earliest costs no syscall at all.
//
// The heap is *indexed*: every entry remembers where it sits in the heap array,
// and the sift operations keep that back-reference current.  This is what makes
// cancellation O(log n) -- a plain std::priority_queue cannot remove an interior
// element, and lazy tombstones would let the heap grow without bound in a
// workload that mostly cancels (which is exactly what a connection timeout is).
class TimerQueue
{
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    // A scheduled timer's identity.  Generation counting means a token from a
    // timer that already fired or was cancelled can never address whichever
    // timer later reuses its slot.
    struct Token
    {
        std::uint32_t index = 0;
        std::uint32_t generation = 0;

        bool IsValid() const noexcept { return generation != 0; }
        friend bool operator==(const Token&, const Token&) = default;
    };

    TimerQueue() = default;

    TimerQueue(const TimerQueue&) = delete;
    TimerQueue& operator=(const TimerQueue&) = delete;

    Timer::HandleType GetNativeHandle() const noexcept
    {
        return timer_.GetNativeHandle();
    }

    // Register a resumption for *deadline*.  Re-arms the fd only when this
    // becomes the new earliest deadline.
    Token Schedule(TimePoint deadline, DelayedOperation* operation)
    {
        const std::uint32_t index = AcquireEntry();
        Entry& entry = entries_[index];
        entry.deadline = deadline;
        entry.operation = operation;
        entry.active = true;

        heap_.push_back(index);
        Place(heap_.size() - 1, index);
        SiftUp(heap_.size() - 1);
        ++size_;

        Rearm();
        return Token {.index = index, .generation = entry.generation};
    }

    // Withdraw a scheduled timer.  Returns false for a token that has already
    // fired or been cancelled, which is not an error: the caller often cannot
    // know which happened first.
    bool Cancel(Token token) noexcept
    {
        Entry* entry = Find(token);
        if (entry == nullptr)
        {
            return false;
        }
        RemoveFromHeap(entry->heap_position);
        ReleaseEntry(token.index);
        --size_;
        Rearm();
        return true;
    }

    // Collect every timer whose deadline has passed and re-arm for the next one.
    //
    // Operations are appended rather than resumed here: resuming one may
    // schedule further timers, and mutating the heap in the middle of walking it
    // would be a reentrancy bug.  The caller completes and resumes afterwards.
    void DrainExpired(std::vector<DelayedOperation*>& out)
    {
        // Consume the fd's expiration counter first; a timerfd stays readable
        // until read, so skipping this would spin a level-triggered poller.
        timer_.Drain();

        const TimePoint now = Clock::now();
        while (!heap_.empty())
        {
            const std::uint32_t index = heap_.front();
            if (entries_[index].deadline > now)
            {
                break;
            }
            out.push_back(entries_[index].operation);
            RemoveFromHeap(0);
            ReleaseEntry(index);
            --size_;
        }
        Rearm();
    }

    std::size_t Size() const noexcept { return size_; }
    bool Empty() const noexcept { return size_ == 0; }

    std::optional<TimePoint> GetEarliestDeadline() const noexcept
    {
        if (heap_.empty())
        {
            return std::nullopt;
        }
        return entries_[heap_.front()].deadline;
    }

    bool Contains(Token token) const noexcept
    {
        return Find(token) != nullptr;
    }

private:
    static constexpr std::uint32_t kNoIndex = std::numeric_limits<std::uint32_t>::max();

    struct Entry
    {
        TimePoint deadline {};
        DelayedOperation* operation = nullptr;
        // A slot's first token is generation 1, so 0 can mean "no timer".
        std::uint32_t generation = 1;
        std::uint32_t heap_position = kNoIndex;
        std::uint32_t next_free = kNoIndex;
        bool active = false;
    };

    const Entry* Find(Token token) const noexcept
    {
        if (!token.IsValid() || token.index >= entries_.size())
        {
            return nullptr;
        }
        const Entry& entry = entries_[token.index];
        if (!entry.active || entry.generation != token.generation)
        {
            return nullptr;
        }
        return &entry;
    }

    Entry* Find(Token token) noexcept
    {
        return const_cast<Entry*>(std::as_const(*this).Find(token));
    }

    std::uint32_t AcquireEntry()
    {
        if (free_head_ != kNoIndex)
        {
            const std::uint32_t index = free_head_;
            free_head_ = entries_[index].next_free;
            entries_[index].next_free = kNoIndex;
            return index;
        }
        entries_.emplace_back();
        return static_cast<std::uint32_t>(entries_.size() - 1);
    }

    void ReleaseEntry(std::uint32_t index) noexcept
    {
        Entry& entry = entries_[index];
        entry.active = false;
        entry.operation = nullptr;
        entry.heap_position = kNoIndex;
        // Invalidate every token ever minted for this slot.
        ++entry.generation;
        if (entry.generation == 0)
        {
            ++entry.generation;
        }
        entry.next_free = free_head_;
        free_head_ = index;
    }

    // Write an entry into a heap position, keeping the entry's back-reference
    // in step.  Every heap mutation goes through here, which is what keeps
    // heap_position trustworthy enough to cancel by token.
    void Place(std::size_t position, std::uint32_t index) noexcept
    {
        heap_[position] = index;
        entries_[index].heap_position = static_cast<std::uint32_t>(position);
    }

    bool IsEarlier(std::uint32_t left, std::uint32_t right) const noexcept
    {
        return entries_[left].deadline < entries_[right].deadline;
    }

    bool SiftUp(std::size_t position) noexcept
    {
        const std::uint32_t index = heap_[position];
        bool moved = false;
        while (position > 0)
        {
            const std::size_t parent = (position - 1) / 2;
            if (!IsEarlier(index, heap_[parent]))
            {
                break;
            }
            Place(position, heap_[parent]);
            position = parent;
            moved = true;
        }
        if (moved)
        {
            Place(position, index);
        }
        return moved;
    }

    void SiftDown(std::size_t position) noexcept
    {
        const std::uint32_t index = heap_[position];
        const std::size_t count = heap_.size();
        while (true)
        {
            const std::size_t left = position * 2 + 1;
            if (left >= count)
            {
                break;
            }
            const std::size_t right = left + 1;
            const std::size_t child = right < count && IsEarlier(heap_[right], heap_[left]) ? right : left;
            if (!IsEarlier(heap_[child], index))
            {
                break;
            }
            Place(position, heap_[child]);
            position = child;
        }
        Place(position, index);
    }

    void RemoveFromHeap(std::uint32_t position) noexcept
    {
        const std::size_t last = heap_.size() - 1;
        if (position == last)
        {
            heap_.pop_back();
            return;
        }
        Place(position, heap_[last]);
        heap_.pop_back();
        // The replacement came from the bottom, so it may belong either above or
        // below its new position; only one of the two can actually move it.
        if (!SiftUp(position))
        {
            SiftDown(position);
        }
    }

    // Point the fd at the earliest deadline.  Skipped when that deadline has not
    // changed, so scheduling behind the current head costs no syscall.
    void Rearm()
    {
        if (heap_.empty())
        {
            if (armed_.has_value())
            {
                timer_.Disarm();
                armed_.reset();
            }
            return;
        }
        const TimePoint earliest = entries_[heap_.front()].deadline;
        if (armed_.has_value() && *armed_ == earliest)
        {
            return;
        }
        timer_.SetDeadline(earliest);
        armed_ = earliest;
    }

    Timer timer_;
    std::vector<Entry> entries_;
    // Heap of indices into entries_.  Storing indices rather than entries keeps
    // an entry's identity stable while it moves around the heap.
    std::vector<std::uint32_t> heap_;
    std::uint32_t free_head_ = kNoIndex;
    std::size_t size_ = 0;
    std::optional<TimePoint> armed_;
};
} // namespace KV
