#pragma once
#if defined(__linux__)

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/RDMA_Acceptor.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "Channel.hpp"
#include "RDMA_Session.hpp"

#include <exception>
#include <memory>
#include <utility>

namespace Foundation::NBIO
{
class RDMA_AcceptChannel final : public Channel
{
  public:
    using Handle = int;

    struct PendingAccept
    {
        std::shared_ptr<RDMA_Session> session{};
        std::exception_ptr error{};
    };

    RDMA_AcceptChannel(Foundation::Core::RDMA_Acceptor &acceptor, Multiplexer &multiplexer,
                       Foundation::Async::Scheduler &scheduler);
    ~RDMA_AcceptChannel() noexcept;

    Task<std::shared_ptr<RDMA_Session>> accept();

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

    PendingAccept &job() noexcept
    {
        return job_;
    }

    const PendingAccept &job() const noexcept
    {
        return job_;
    }

    Foundation::Core::RDMA_Acceptor &acceptor() noexcept
    {
        return acceptor_;
    }

  private:
    Foundation::Core::RDMA_Acceptor &acceptor_;
    PendingAccept job_{};
    Foundation::Async::Coroutine waiter_{};
    bool submitted_{false};
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)
