#include "RDMA_AcceptChannel.hpp"

#include <Foundation/Async/Coroutine.hpp>

#include <utility>

namespace Foundation::NBIO
{
namespace detail
{
class RDMA_AcceptAwaiter
{
  public:
    explicit RDMA_AcceptAwaiter(RDMA_AcceptChannel &channel) : channel_(channel)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        channel_.job().session.reset();
        channel_.job().error = {};
        channel_.park(Foundation::Async::Coroutine::from_handle(handle));
        channel_.arm();
        return true;
    }

    std::shared_ptr<RDMA_Session> await_resume()
    {
        auto &job = channel_.job();
        if (job.error)
        {
            std::rethrow_exception(job.error);
        }
        return std::move(job.session);
    }

  private:
    RDMA_AcceptChannel &channel_;
};
} // namespace detail

RDMA_AcceptChannel::RDMA_AcceptChannel(Foundation::Core::RDMA_Acceptor &acceptor, Multiplexer &multiplexer,
                                       Foundation::Async::Scheduler &scheduler) :
    Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRDMA_Accept, static_cast<std::uintptr_t>(acceptor.native_handle()), multiplexer, scheduler),
    acceptor_(acceptor)
{
    acceptor_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

RDMA_AcceptChannel::~RDMA_AcceptChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

Task<std::shared_ptr<RDMA_Session>> RDMA_AcceptChannel::accept()
{
    co_return co_await detail::RDMA_AcceptAwaiter{*this};
}

void RDMA_AcceptChannel::handle_completion()
{
    if (!waiter_) [[unlikely]]
    {
        return;
    }

    try
    {
        if (auto stream = acceptor_.accept())
        {
            job().session = std::make_shared<RDMA_Session>(std::move(*stream), multiplexer_, scheduler_);
            job().error = {};
            auto waiter = std::exchange(waiter_, {});
            scheduler_.submit(std::move(waiter));
            return;
        }
    }
    catch (...)
    {
        job().error = std::current_exception();
        auto waiter = std::exchange(waiter_, {});
        scheduler_.submit(std::move(waiter));
        return;
    }

    // Nothing has asked to be admitted yet -- or the event was about a
    // connection that is already established. Either way the waiter stays
    // parked and the channel keeps watching.
    job().session.reset();
    job().error = {};
    arm();
}
} // namespace Foundation::NBIO
