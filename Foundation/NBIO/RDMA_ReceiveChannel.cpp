#include "RDMA_ReceiveChannel.hpp"

#include <Foundation/Async/Coroutine.hpp>

#include <cassert>
#include <utility>

namespace Foundation::NBIO
{
namespace detail
{
class RDMA_ReceiveAwaiter
{
  public:
    RDMA_ReceiveAwaiter(RDMA_ReceiveChannel &channel) : channel_(channel)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        // Reap whatever has already completed before parking. The completion
        // channel is shared with the send half, so a completion can land
        // without this channel's event ever reaching the multiplexer: the send
        // half's poll drains the shared channel, and the entry is then sitting
        // in the receive queue with nothing left to wake us for it.
        (void)channel_.stream().poll_receive(0);
        if (auto chunk = channel_.stream().receive())
        {
            channel_.job().chunk = *chunk;
            return false;
        }

        channel_.job().chunk = std::nullopt;
        channel_.park(Foundation::Async::Coroutine::from_handle(handle));
        channel_.arm();
        return true;
    }

    std::optional<std::span<char>> await_resume() const noexcept
    {
        return channel_.job().chunk;
    }

  private:
    RDMA_ReceiveChannel &channel_;
};

// The same, but for a caller that is willing to carry on without one. It never
// parks, so it must not be used while another coroutine is waiting on the same
// channel: there is one job per channel, and this one would take it.
class RDMA_ReceiveTryAwaiter
{
  public:
    explicit RDMA_ReceiveTryAwaiter(RDMA_ReceiveChannel &channel) : channel_(channel)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType>) noexcept
    {
        // Still has to reap: a completion that is already in the queue would
        // otherwise sit there with nobody left to deliver it, because nobody is
        // parking to be woken for it.
        (void)channel_.stream().poll_receive(0);
        channel_.job().chunk = channel_.stream().receive();
        return false;
    }

    std::optional<std::span<char>> await_resume() const noexcept
    {
        return channel_.job().chunk;
    }

  private:
    RDMA_ReceiveChannel &channel_;
};
} // namespace detail

RDMA_ReceiveChannel::RDMA_ReceiveChannel(Foundation::Core::RDMA_Stream &stream, Multiplexer &multiplexer,
                                         Foundation::Async::Scheduler &scheduler) :
    Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRDMA_Receive, stream.native_handle(), multiplexer,
                              scheduler),
    stream_(stream)
{
    multiplexer_.add_channel(this);
}

RDMA_ReceiveChannel::~RDMA_ReceiveChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

Foundation::NBIO::Task<std::optional<std::span<char>>> RDMA_ReceiveChannel::receive()
{
    co_return co_await detail::RDMA_ReceiveAwaiter{*this};
}

Foundation::NBIO::Task<std::optional<std::span<char>>> RDMA_ReceiveChannel::try_receive()
{
    co_return co_await detail::RDMA_ReceiveTryAwaiter{*this};
}

void RDMA_ReceiveChannel::release(std::span<char> chunk)
{
    stream_.release(chunk);
}

void RDMA_ReceiveChannel::handle_event()
{
    if (handler_) [[likely]]
    {
        handler_(this);
    }

    if (!waiter_) [[unlikely]]
    {
        return;
    }

    const int delivered = stream_.poll_receive(0);
    if (delivered == 0 && !stream_.peer_closed() && !stream_.error())
    {
        arm();
        return;
    }

    if (auto chunk = stream_.receive())
    {
        job().chunk = *chunk;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}
} // namespace Foundation::NBIO