#pragma once
#if defined(__linux__)

#include <Foundation/Core/RDMA_Stream.hpp>
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include "Channel.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <span>
#include <system_error>
#include <utility>

namespace Foundation::NBIO
{
class RDMA_SendChannel final : public Channel
{
  public:
    using Handle = int;

    struct PendingSend
    {
        // How many sends may still be in flight for the parked poll to be met.
        std::size_t target{0};
        // Completions reaped since the poll began.
        std::size_t completions{0};
    };

    RDMA_SendChannel(Foundation::Core::RDMA_Stream &stream, Multiplexer &multiplexer,
             Foundation::Async::Scheduler &scheduler);
    ~RDMA_SendChannel() noexcept;

    // A chunk to fill. Several can be held at once, so the way to use this is to
    // take as many as the stream will give, fill them, and send them -- the
    // device carries them in parallel instead of one per round trip.
    std::optional<std::span<char>> acquire() noexcept
    {
        return stream_.acquire();
    }

    // Hands one acquired chunk to the device. Returns immediately: the chunk
    // belongs to the device until a completion retires it, which poll() reports.
    std::error_code send(std::span<char> chunk, std::size_t length)
    {
        return stream_.send(chunk, length);
    }

    // Waits until at least `count` of the sends in flight when it was called have
    // completed, or -- for the default -- until all of them have, after which
    // every chunk has been handed back. Answers how many completed while it
    // waited, which is also how many chunks became available.
    Foundation::NBIO::Task<std::size_t> poll(std::size_t count = 0);

    // Sends posted and not yet reaped.
    std::size_t outstanding() const noexcept
    {
        return stream_.outstanding_sends();
    }

    void handle_completion();

    // The one-job protocol (see Channel.hpp): the parked wait is the job, submitting
    // hands its poll to the backend, and the poll's completion erases it.
    bool submit_job() noexcept
    {
        if (submitted_)
        {
            return false; // its poll is already out there
        }
        submitted_ = true;
        return true;
    }

    void advance_job(std::ptrdiff_t) noexcept
    {
        // A poll's answer says only that the fd became readable; what that means is
        // reaped in handle_completion().
    }

    void complete_job() noexcept
    {
        submitted_ = false;
    }

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    Foundation::Core::RDMA_Stream &stream() noexcept
    {
        return stream_;
    }

    PendingSend &job() noexcept
    {
      return job_;
    }

    const PendingSend &job() const noexcept
    {
      return job_;
    }

    // Completions reaped so far, which a poll reads to answer with a difference.
    std::size_t &completed() noexcept
    {
        return completed_;
    }

  private:
    Foundation::Core::RDMA_Stream &stream_;
    PendingSend job_{};
    std::size_t completed_{0};
    Foundation::Async::Coroutine waiter_{};
    bool submitted_{false};
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)
