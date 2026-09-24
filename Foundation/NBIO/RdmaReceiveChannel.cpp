#include "RdmaReceiveChannel.hpp"

#include <Foundation/Async/Coroutine.hpp>

#include <cassert>
#include <cstdint>
#include <utility>

namespace Foundation::NBIO
{
namespace
{
// Reaps the completion queue and answers the next received chunk. The
// completion channel is shared with the send half, so a completion can land
// without this channel's event ever reaching the multiplexer: the send half's
// poll drains the shared channel, and the entry is then sitting in the receive
// queue with nothing left to wake a parked receive for it. Reaping first, every
// time, is what keeps that from becoming a stall.
Core::expected<std::optional<std::span<char>>, std::string> Reap(RdmaReceiveChannel &channel)
{
    if (auto reaped = channel.connection().poll_receive(0); !reaped) [[unlikely]]
    {
        return Core::unexpected(reaped.error());
    }
    return channel.connection().receive();
}
} // namespace

namespace detail
{
class RdmaReceiveAwaiter
{
  public:
    RdmaReceiveAwaiter(RdmaReceiveChannel &channel) : channel_(channel)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        auto chunk = Reap(channel_);
        if (!chunk)
        {
            // Nothing will arrive on a stream that has failed, so parking would
            // leave the waiter there for good.
            channel_.job().error = chunk.error();
            return false;
        }
        if (*chunk)
        {
            channel_.job().chunk = std::move(*chunk);
            channel_.job().error.clear();
            return false;
        }

        channel_.job().chunk = std::nullopt;
        channel_.job().error.clear();
        channel_.park(Foundation::Async::Coroutine::from_handle(handle));
        channel_.arm();
        return true;
    }

    Core::expected<std::optional<std::span<char>>, std::string> await_resume() const
    {
        auto &job = channel_.job();
        if (!job.error.empty()) [[unlikely]]
        {
            // Consumed, so a later receive on this channel starts clean.
            return Core::unexpected(std::exchange(job.error, std::string{}));
        }
        return job.chunk;
    }

  private:
    RdmaReceiveChannel &channel_;
};

// The same, but for a caller that is willing to carry on without one. It never
// parks, so it must not be used while another coroutine is waiting on the same
// channel: there is one job per channel, and this one would take it.
class RdmaReceiveTryAwaiter
{
  public:
    explicit RdmaReceiveTryAwaiter(RdmaReceiveChannel &channel) : channel_(channel)
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
        auto chunk = Reap(channel_);
        if (!chunk) [[unlikely]]
        {
            channel_.job().error = chunk.error();
            return false;
        }
        channel_.job().error.clear();
        channel_.job().chunk = std::move(*chunk);
        return false;
    }

    Core::expected<std::optional<std::span<char>>, std::string> await_resume() const
    {
        auto &job = channel_.job();
        if (!job.error.empty()) [[unlikely]]
        {
            return Core::unexpected(std::exchange(job.error, std::string{}));
        }
        return job.chunk;
    }

  private:
    RdmaReceiveChannel &channel_;
};
} // namespace detail

RdmaReceiveChannel::RdmaReceiveChannel(Foundation::Core::RdmaConnector &connection, Multiplexer &multiplexer,
                                         Foundation::Async::Scheduler &scheduler) :
    Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRdmaReceive, static_cast<std::uintptr_t>(connection.native_handle()), multiplexer, scheduler),
    connection_(connection)
{
    // Registered on the first arm(): nothing to watch until a receive is parked.
}

RdmaReceiveChannel::~RdmaReceiveChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

Foundation::NBIO::Task<Core::expected<std::optional<std::span<char>>, std::string>> RdmaReceiveChannel::receive()
{
    co_return co_await detail::RdmaReceiveAwaiter{*this};
}

Foundation::NBIO::Task<Core::expected<std::optional<std::span<char>>, std::string>> RdmaReceiveChannel::try_receive()
{
    co_return co_await detail::RdmaReceiveTryAwaiter{*this};
}

Core::expected<void, std::string> RdmaReceiveChannel::release(std::span<char> chunk) noexcept
{
    return connection_.release(chunk);
}

Payload &RdmaReceiveChannel::submit()
{
    return payload_;
}

void RdmaReceiveChannel::complete()
{
    auto &payload = std::get<RdmaReceivePayload>(payload_);
    payload.release_poll();

    if (!waiter_) [[unlikely]]
    {
        return;
    }

    auto delivered = connection_.poll_receive(0);
    if (!delivered) [[unlikely]]
    {
        job().error = delivered.error();
    }
    else if (*delivered == 0 && !connection_.peer_closed() && !connection_.failed())
    {
        // Nothing came back and nothing is wrong: the event belonged to the send
        // half, so keep waiting for a receive.
        arm();
        return;
    }
    else if (auto received = connection_.receive(); received && *received)
    {
        job().chunk = std::move(*received);
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}
} // namespace Foundation::NBIO
