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

    struct AcceptJob
    {
        std::shared_ptr<RDMA_Session> session{};
        std::exception_ptr error{};
    };

    RDMA_AcceptChannel(Foundation::Core::RDMA_Acceptor &acceptor, Multiplexer &multiplexer,
                       Foundation::Async::Scheduler &scheduler);
    ~RDMA_AcceptChannel() noexcept;

    Task<std::shared_ptr<RDMA_Session>> accept();

    void handle_completion();

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    AcceptJob &job() noexcept
    {
        return job_;
    }

    const AcceptJob &job() const noexcept
    {
        return job_;
    }

    Foundation::Core::RDMA_Acceptor &acceptor() noexcept
    {
        return acceptor_;
    }

  private:
    Foundation::Core::RDMA_Acceptor &acceptor_;
    AcceptJob job_{};
    Foundation::Async::Coroutine waiter_{};
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)
