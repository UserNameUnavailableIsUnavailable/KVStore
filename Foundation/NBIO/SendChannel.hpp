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

    // The batch protocol (see Channel.hpp). One operation is one sendmsg over the
    // whole prepared prefix, because a stream is written in order.
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
    friend class SendAwaiter;

    // Queues the send and arms the channel: this is the suspension point, and
    // being armed is what tells the backend to look at the channel.
    void prepare(std::span<const char> buffer, Foundation::Core::SendResult &result, Foundation::Async::Coroutine waiter);

    // Gives one job its verdict and moves it to the completed queue. How much of it
    // went out is already counted in its outcome slot by the time this runs.
    void retire(PendingSend job, Foundation::Core::SendStatus status, std::error_code error) noexcept;

    void drop(PendingSend &pending) noexcept;

    Foundation::Core::Socket &socket_;
    // The queue, in the order the sends were awaited. What one operation covers is
    // a prefix of it: those jobs are held in `submitted_jobs_` while the operation
    // is out there, and in `completed_jobs_` once they are whole. A send the
    // operation stopped inside stays in `submitted_jobs_`, at the front, holding
    // what is left of its buffer.
    std::deque<PendingSend> prepared_jobs_;
    std::deque<PendingSend> submitted_jobs_;
    std::deque<PendingSend> completed_jobs_;
#if defined(__unix__)
    std::vector<::iovec> vectors_;
    ::msghdr message_header_{};
#endif
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
