#include "RDMA_ConnectChannel.hpp"

#include <Foundation/Async/Coroutine.hpp>

#include <utility>

namespace Foundation::NBIO
{
namespace detail
{
class RDMA_ConnectAwaiter
{
  public:
    RDMA_ConnectAwaiter(RDMA_ConnectChannel &channel, Foundation::Core::Address peer) :
        channel_(channel), peer_(std::move(peer))
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType>)
    {
        channel_.job().session.reset();
        channel_.job().error = {};

        try
        {
            channel_.job().session = std::make_shared<RDMA_Session>(channel_.connector().connect(peer_),
                                                                    channel_.multiplexer(), channel_.scheduler());
        }
        catch (...)
        {
            channel_.job().error = std::current_exception();
        }

        return false;
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
    RDMA_ConnectChannel &channel_;
    Foundation::Core::Address peer_;
};
} // namespace detail

RDMA_ConnectChannel::RDMA_ConnectChannel(Foundation::Core::RDMA_Connector &connector, Multiplexer &multiplexer,
                                         Foundation::Async::Scheduler &scheduler) :
    Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRDMA_Connect, connector.native_handle(), multiplexer,
                              scheduler),
    connector_(connector)
{
    connector_.native_handle();
    multiplexer_.add_channel(this);
}

RDMA_ConnectChannel::~RDMA_ConnectChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

Task<std::shared_ptr<RDMA_Session>> RDMA_ConnectChannel::connect(Foundation::Core::Address peer)
{
    co_return co_await detail::RDMA_ConnectAwaiter{*this, std::move(peer)};
}

void RDMA_ConnectChannel::handle_event()
{
    if (handler_) [[likely]]
    {
        handler_(this);
    }

    if (!waiter_) [[unlikely]]
    {
        return;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}
} // namespace Foundation::NBIO