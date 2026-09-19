#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <deque>
#include <span>
#include <sys/uio.h>
#include <system_error>
#include <vector>

namespace Foundation::NBIO
{
class ReceiveAwaiter;

// One receive waiting its turn. The buffer and the slot its outcome is written
// into belong to the frame that is parked, which is alive for exactly as long as
// this entry is queued, so the entry points at them rather than owning them.
class PendingReceive
{
  public:
    PendingReceive(std::span<char> buffer, Foundation::Core::ReceiveResult &result, Foundation::Async::Coroutine waiter) noexcept
        : buffer_(buffer), result_(&result), waiter_(std::move(waiter))
    {
    }

    std::span<char> &buffer() noexcept
    {
        return buffer_;
    }
    const std::span<char> &buffer() const noexcept
    {
        return buffer_;
    }

    Foundation::Core::ReceiveResult &result() noexcept
    {
        return *result_;
    }
    const Foundation::Core::ReceiveResult &result() const noexcept
    {
        return *result_;
    }

    Foundation::Async::Coroutine &waiter() noexcept
    {
        return waiter_;
    }

  private:
    std::span<char> buffer_;
    Foundation::Core::ReceiveResult *result_{nullptr};
    Foundation::Async::Coroutine waiter_;
};

// Simplex channel dedicated to receiving: a queue of receives, one operation, with
// the channel interested only in the "readable" event.
//
// There is one queue, not three. A queued receive is never dropped until it has an
// answer, so the receives the backend has been given are simply the first
// `submitted_` entries of `pending_` -- a prefix, which is exactly what a
// vectorised read needs, because the kernel fills the buffers in the order they are
// given and answers with one total. Keeping that prefix implicit is what lets a
// short read land in the middle of the queue without anything being moved.
class ReceiveChannel : public Foundation::NBIO::Channel
{
  public:
    explicit ReceiveChannel(Foundation::Core::Socket &socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~ReceiveChannel() noexcept;

    Foundation::NBIO::Task<std::optional<std::size_t>> receive(std::span<char> buffer);

    // Wakes what the operation answered and decides what the backend owes next.
    void handle_completion();

    // Work the backend could take right now: there is something queued and no
    // operation of ours is with the kernel.
    bool has_prepared() const noexcept
    {
        return submitted_ == 0 && !pending_.empty();
    }

    // Hands the prepared prefix over as one operation: builds the iovecs, records
    // how many receives it covers, and answers that count. Zero means there was
    // nothing to hand over.
    std::size_t count_prepared() noexcept;

    // The operation in flight reported its outcome.
    void complete_tasks(std::ptrdiff_t result) noexcept;

    // Is an operation of this channel's with the kernel?
    bool has_submitted() const noexcept
    {
        return submitted_ != 0;
    }

    const ::msghdr &message_batch() const noexcept
    {
        return message_header_;
    }
    ::msghdr &message_batch() noexcept
    {
        return message_header_;
    }

    Foundation::Core::Socket &socket() noexcept
    {
        return socket_;
    }
    const Foundation::Core::Socket &socket() const noexcept
    {
        return socket_;
    }

    std::error_code last_error() const noexcept
    {
        return error_code_;
    }

  private:
    friend class ReceiveAwaiter;

    void prepare(std::span<char> buffer, Foundation::Core::ReceiveResult &result, Foundation::Async::Coroutine waiter);

    // Arms or disarms according to what the backend still owes. A readiness
    // backend hands the work over here, because its readiness is the submission.
    void refresh_arming() noexcept;

    // Retires the answered receives at the front of the queue, in the order the
    // stream filled them, and hands their waiters back to the scheduler.
    void retire_answered() noexcept;

    void drop(PendingReceive &pending) noexcept;

    Foundation::Core::Socket &socket_;
    std::deque<PendingReceive> pending_;
    // How many of `pending_`, from the front, the kernel has been given.
    std::size_t submitted_{0};
    std::vector<::iovec> io_vectors_;
    ::msghdr message_header_{};
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
