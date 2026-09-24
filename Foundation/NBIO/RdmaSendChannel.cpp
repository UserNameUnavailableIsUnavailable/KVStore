#include "RdmaSendChannel.hpp"

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
class RdmaSendPollAwaiter
{
  public:
    RdmaSendPollAwaiter(RdmaSendChannel &channel, std::size_t count) : channel_(channel)
    {
        const std::size_t outstanding = channel_.outstanding();
        // count == 0 means "all of them", so the target is nothing still in
        // flight when the wait began.
        target_ = count == 0 ? 0 : (outstanding > count ? outstanding - count : 0);
        before_ = channel_.completed();
    }

    bool await_ready() const noexcept
    {
        if (channel_.outstanding() <= target_)
        {
            // Nothing to wait for, so nothing to report either: a failure left
            // over from an earlier poll must not be read as this poll's answer.
            channel_.job().error.clear();
            return true;
        }
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        // Reap what has already completed before parking. The completion channel
        // is shared with the receive half, so a completion can land without this
        // channel's event ever reaching the multiplexer.
        if (auto reaped = channel_.connection().poll_send(0); reaped) [[likely]]
        {
            channel_.completed() += *reaped;
        }
        else
        {
            // The stream will not complete anything again. Answering now, rather
            // than parking, is what keeps a waiter from being parked for good.
            channel_.job().error = reaped.error();
            return false;
        }
        if (channel_.outstanding() <= target_)
        {
            return false;
        }

        channel_.job() = {.target = target_, .completions = 0, .error = {}};
        channel_.park(Foundation::Async::Coroutine::from_handle(handle));
        channel_.arm();
        return true;
    }

    Core::expected<std::size_t, std::string> await_resume() const
    {
        auto &job = channel_.job();
        if (!job.error.empty()) [[unlikely]]
        {
            // Consumed, so that a later poll on the same channel starts clean.
            return Core::unexpected(std::exchange(job.error, std::string{}));
        }
        return channel_.completed() - before_;
    }

  private:
    RdmaSendChannel &channel_;
    std::size_t target_{0};
    std::size_t before_{0};
};
} // namespace detail

RdmaSendChannel::RdmaSendChannel(Foundation::Core::RdmaConnector &connection, Multiplexer &multiplexer,
                                   Foundation::Async::Scheduler &scheduler) :
    Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRdmaSend, static_cast<std::uintptr_t>(connection.native_handle()), multiplexer, scheduler),
    connection_(connection)
{
    // Registered on the first arm(): nothing to watch until a poll is parked.
}

RdmaSendChannel::~RdmaSendChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::string>> RdmaSendChannel::poll(std::size_t count)
{
    co_return co_await detail::RdmaSendPollAwaiter{*this, count};
}

Payload &RdmaSendChannel::submit()
{
    return payload_;
}

void RdmaSendChannel::complete()
{
    auto &payload = std::get<RdmaSendPayload>(payload_);
    payload.release_poll();

    if (!waiter_) [[unlikely]]
    {
        // Nothing is waiting, so nothing may be reaped: a completion taken here
        // would have no poll to belong to.
        return;
    }

    if (auto reaped = connection_.poll_send(0); reaped) [[likely]]
    {
        completed_ += *reaped;
        if (connection_.outstanding_sends() > job().target && !connection_.peer_closed() && !connection_.failed())
        {
            arm();
            return;
        }
    }
    else
    {
        job().error = reaped.error();
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}
} // namespace Foundation::NBIO
