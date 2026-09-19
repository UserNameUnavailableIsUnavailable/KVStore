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
// The queue is explicit: a receive waits in `prepared_jobs_`, the prefix one
// operation covers is held in `submitted_jobs_` while it is out there, and what it
// answered is in `completed_jobs_` waiting to be woken. A short read answers the
// front of the batch and leaves the rest where they are -- the socket is a stream,
// and not having more to give is not the end of it.
class ReceiveChannel : public Foundation::NBIO::Channel
{
  public:
    explicit ReceiveChannel(Foundation::Core::Socket &socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~ReceiveChannel() noexcept;

    Foundation::NBIO::Task<std::optional<std::size_t>> receive(std::span<char> buffer);

    // The batch protocol (see Channel.hpp). One operation is one recvmsg over the
    // whole prepared prefix, because a stream is read in order.
    ::msghdr *submit_jobs();
    void advance_job(std::ptrdiff_t result) noexcept;
    void complete_jobs() noexcept;
    void handle_completion();

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

    // Queues the receive and arms the channel: this is the suspension point, and
    // being armed is what tells the backend to look at the channel.
    void prepare(std::span<char> buffer, Foundation::Core::ReceiveResult &result, Foundation::Async::Coroutine waiter);

    // Gives one job its answer and moves it to the completed queue.
    void retire(PendingReceive job, Foundation::Core::ReceiveResult result) noexcept;

    void drop(PendingReceive &pending) noexcept;

    Foundation::Core::Socket &socket_;
    // The queue, in the order the receives were awaited. What one operation covers
    // is a prefix of it: those jobs are held in `submitted_jobs_` while the
    // operation is out there, and in `completed_jobs_` once they have an answer.
    std::deque<PendingReceive> prepared_jobs_;
    std::deque<PendingReceive> submitted_jobs_;
    std::deque<PendingReceive> completed_jobs_;
    std::vector<::iovec> io_vectors_;
    ::msghdr message_header_{};
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
