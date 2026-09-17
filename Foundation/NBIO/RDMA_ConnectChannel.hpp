#pragma once
#if defined(__linux__)

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Address.hpp>
#include <Foundation/Core/RDMA_Connector.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "Channel.hpp"
#include "RDMA_Session.hpp"

#include <exception>
#include <memory>
#include <utility>

namespace Foundation::NBIO
{
class RDMA_ConnectChannel final : public Channel
{
  public:
    using Handle = int;

    struct ConnectJob
    {
        std::shared_ptr<RDMA_Session> session{};
        std::exception_ptr error{};
    };

    RDMA_ConnectChannel(Foundation::Core::RDMA_Connector &connector, Multiplexer &multiplexer,
                        Foundation::Async::Scheduler &scheduler);
    ~RDMA_ConnectChannel() noexcept override;

    Task<std::shared_ptr<RDMA_Session>> connect(Foundation::Core::Address peer);

    void handle_event() override;

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    ConnectJob &job() noexcept
    {
        return job_;
    }

    const ConnectJob &job() const noexcept
    {
        return job_;
    }

    Foundation::Core::RDMA_Connector &connector() noexcept
    {
        return connector_;
    }

    Handle native_handle() const noexcept
    {
        return connector_.native_handle();
    }

  private:
    Foundation::Core::RDMA_Connector &connector_;
    Foundation::Core::Address peer_{};
    ConnectJob job_{};
    Foundation::Async::Coroutine waiter_{};
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)