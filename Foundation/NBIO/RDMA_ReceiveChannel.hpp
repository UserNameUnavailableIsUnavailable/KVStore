#pragma once
#if defined(__linux__)

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/RDMA_Stream.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "Channel.hpp"

#include <optional>
#include <span>
#include <utility>

namespace Foundation::NBIO
{
class RDMA_ReceiveChannel final : public Channel
{
  public:
    using Handle = int;

        struct PendingReceive
        {
                std::optional<std::span<char>> chunk{};
        };

    ~RDMA_ReceiveChannel() noexcept;

    RDMA_ReceiveChannel(Foundation::Core::RDMA_Stream &stream, Multiplexer &multiplexer,
                        Foundation::Async::Scheduler &scheduler);

    Foundation::NBIO::Task<std::optional<std::span<char>>> receive();

    Foundation::NBIO::Task<std::optional<std::span<char>>> try_receive();
    void release(std::span<char> chunk);

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

    PendingReceive &job() noexcept
    {
        return job_;
    }

    const PendingReceive &job() const noexcept
    {
        return job_;
    }

    private:
    Foundation::Core::RDMA_Stream &stream_;
    PendingReceive job_{};
    Foundation::Async::Coroutine waiter_{};
    bool submitted_{false};
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)
