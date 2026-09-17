#include "RDMA_SendChannel.hpp"

#include <Foundation/Async/Coroutine.hpp>

#include <cassert>
#include <deque>
#include <utility>

namespace Foundation::NBIO
{
namespace detail
{
// Waits for send completions rather than for one particular send: the caller
// posts several chunks at once, so what it has to know is how many of them the
// device has finished with -- the completions are what hand the chunks back.
class RDMA_SendPollAwaiter
{
  public:
    RDMA_SendPollAwaiter(RDMA_SendChannel &channel, std::size_t count) : channel_(channel)
    {
        const std::size_t outstanding = channel_.outstanding();
        // count == 0 means "all of them", so the target is nothing still in
        // flight when the wait began.
        target_ = count == 0 ? 0 : (outstanding > count ? outstanding - count : 0);
        before_ = channel_.completed();
    }

    bool await_ready() const noexcept
    {
        return channel_.outstanding() <= target_;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        // Reap what has already completed before parking. The completion channel
        // is shared with the receive half, so a completion can land without this
        // channel's event ever reaching the multiplexer.
        channel_.completed() += static_cast<std::size_t>(channel_.stream().poll_send(0));
        if (channel_.outstanding() <= target_)
        {
            return false;
        }

        channel_.job() = {.target = target_, .completions = 0};
        channel_.park(Foundation::Async::Coroutine::from_handle(handle));
        channel_.arm();
        return true;
    }

    std::size_t await_resume() const noexcept
    {
        return channel_.completed() - before_;
    }

  private:
    RDMA_SendChannel &channel_;
    std::size_t target_{0};
    std::size_t before_{0};
};
} // namespace detail

RDMA_SendChannel::RDMA_SendChannel(Foundation::Core::RDMA_Stream &stream, Multiplexer &multiplexer,
                                   Foundation::Async::Scheduler &scheduler) :
    Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRDMA_Send, stream.native_handle(), multiplexer,
                              scheduler),
    stream_(stream)
{
    multiplexer_.add_channel(this);
}

RDMA_SendChannel::~RDMA_SendChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

Foundation::NBIO::Task<std::size_t> RDMA_SendChannel::poll(std::size_t count)
{
    co_return co_await detail::RDMA_SendPollAwaiter{*this, count};
}

void RDMA_SendChannel::handle_event()
{
    if (handler_) [[likely]]
    {
        handler_(this);
    }

    if (!waiter_) [[unlikely]]
    {
        // Nothing is waiting, so nothing may be reaped: a completion taken here
        // would have no poll to belong to.
        return;
    }

    completed_ += static_cast<std::size_t>(stream_.poll_send(0));
    if (stream_.outstanding_sends() > job().target && !stream_.peer_closed() && !stream_.error())
    {
        arm();
        return;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}
} // namespace Foundation::NBIO