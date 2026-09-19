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
#include <sys/socket.h>
#include <sys/uio.h>
#include <system_error>
#include <vector>

struct io_uring_sqe;

namespace Foundation::NBIO
{
class SendAwaiter;

// One send waiting its turn. The bytes and the slot its outcome is written into
// belong to the frame that is parked, which is alive for exactly as long as this
// entry is queued, so the entry points at them rather than owning them.
class PendingSend
{
  public:
    PendingSend(std::span<const char> buffer, Foundation::Core::SendResult &result, Foundation::Async::Coroutine waiter) noexcept
        : buffer_(buffer), result_(&result), waiter_(std::move(waiter))
    {
    }

    std::span<const char> &buffer() noexcept
    {
        return buffer_;
    }
    const std::span<const char> &buffer() const noexcept
    {
        return buffer_;
    }

    Foundation::Core::SendResult &result() noexcept
    {
        return *result_;
    }
    const Foundation::Core::SendResult &result() const noexcept
    {
        return *result_;
    }

    Foundation::Async::Coroutine &waiter() noexcept
    {
        return waiter_;
    }

  private:
    std::span<const char> buffer_;
    Foundation::Core::SendResult *result_{nullptr};
    Foundation::Async::Coroutine waiter_;
};

// Simplex channel dedicated to sending: a queue of sends, one operation, with the
// channel interested only in the "writable" event.
class SendChannel final : public Foundation::NBIO::Channel
{
  public:
    explicit SendChannel(Foundation::Core::Socket &socket, Foundation::Async::Scheduler &scheduler, Foundation::NBIO::Multiplexer &multiplexer);
    ~SendChannel() noexcept;

    Foundation::NBIO::Task<std::optional<std::size_t>> send(std::span<const char> buffer);

    // Sends as much of `buffer` as the socket will take right now, for a caller
    // that would rather not park: a send the kernel takes in one call never has
    // to reach the event loop. Answers the bytes written, or -1 when an operation
    // is already in flight and this send has to queue behind it rather than jump
    // ahead of the bytes already promised to the socket.
    std::ptrdiff_t send_now(std::span<const char> buffer) noexcept;

    // Wakes what the operation finished and decides what the backend owes next.
    void handle_completion();

    // Work the backend could take right now: there is something queued and no
    // operation of ours is with the kernel.
    bool has_prepared() const noexcept
    {
        return submitted_ == 0 && !pending_.empty();
    }

    // Hands the prepared prefix over as one operation: builds the iovecs, records
    // how many sends it covers, and answers that count. Zero means there was
    // nothing to hand over.
    std::size_t count_prepared() noexcept;

    // The operation in flight reported its outcome.
    void complete_tasks(std::ptrdiff_t result) noexcept;

    // Is an operation of this channel's with the kernel? A stream is written in
    // order, so there is at most one outstanding.
    bool has_submitted() const noexcept
    {
        return submitted_ != 0;
    }

#if defined(__linux__)
    const ::msghdr &message_batch() const noexcept
    {
        return message_header_;
    }

    ::msghdr &message_batch() noexcept
    {
        return message_header_;
    }
#endif

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
    friend class SendAwaiter;

    void prepare(std::span<const char> buffer, Foundation::Core::SendResult &result, Foundation::Async::Coroutine waiter);

    void refresh_arming() noexcept;

    // Retires the finished sends at the front of the queue, in the order the
    // stream took them, and hands their waiters back to the scheduler.
    void retire_answered() noexcept;

    void drop(PendingSend &pending) noexcept;

    Foundation::Core::Socket &socket_;
    std::deque<PendingSend> pending_;
    // How many of `pending_`, from the front, the kernel has been given.
    std::size_t submitted_{0};
#if defined(__unix__)
    std::vector<::iovec> vectors_;
    ::msghdr message_header_{};
#endif
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
