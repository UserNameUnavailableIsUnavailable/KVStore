#include "RdmaAcceptChannel.hpp"

#include <Foundation/Async/Coroutine.hpp>

#include <stdexcept>
#include <string>
#include <utility>

namespace Foundation::NBIO
{
namespace detail
{
class RdmaAcceptAwaiter
{
  public:
    explicit RdmaAcceptAwaiter(RdmaAcceptChannel &channel) : channel_(channel)
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

    Core::expected<std::shared_ptr<RdmaSessionService>, std::string> await_resume()
    {
        auto &job = channel_.job();
        if (job.session)
        {
            return std::move(job.session);
        }
        return Core::unexpected(std::exchange(job.error, std::string{}));
    }

  private:
    RdmaAcceptChannel &channel_;
};
} // namespace detail

RdmaAcceptChannel::RdmaAcceptChannel(Foundation::Core::RdmaAcceptor &acceptor, Multiplexer &multiplexer,
                                       Foundation::Async::Scheduler &scheduler) :
    Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRdmaAccept, static_cast<std::uintptr_t>(acceptor.native_handle()), multiplexer, scheduler),
    acceptor_(acceptor)
{
    // The acceptor has to be polled rather than waited on, and this is the one
    // failure the channel cannot report through a job -- there is no waiter to
    // report it to yet. A constructor is the one place still allowed to throw.
    if (auto armed = acceptor_.non_blocking(true); !armed) [[unlikely]]
    {
        throw std::runtime_error("Failed to make the RDMA acceptor non-blocking: " + armed.error());
    }
    // Registered on the first arm(): nothing to watch until a wait queues.
}

RdmaAcceptChannel::~RdmaAcceptChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

Task<Core::expected<std::shared_ptr<RdmaSessionService>, std::string>> RdmaAcceptChannel::accept()
{
    co_return co_await detail::RdmaAcceptAwaiter{*this};
}

Payload &RdmaAcceptChannel::submit()
{
    return payload_;
}

void RdmaAcceptChannel::complete()
{
    auto &payload = std::get<RdmaAcceptPayload>(payload_);
    payload.release_poll();

    if (!waiter_) [[unlikely]]
    {
        return;
    }

    auto accepted = acceptor_.accept();
    if (!accepted) [[unlikely]]
    {
        job().session.reset();
        job().error = accepted.error();
    }
    else if (*accepted)
    {
        // Building the session allocates its two channels, which is the one step
        // left that can throw. Failing to build it belongs to this accept, not to
        // whoever is waiting on the engine.
        try
        {
            job().session = std::make_shared<RdmaSessionService>(std::move(**accepted), multiplexer_, scheduler_);
            job().error = {};
        }
        catch (const std::exception &failure)
        {
            job().session.reset();
            job().error = std::string{ failure.what() };
        }
    }
    else
    {
        // Nothing has asked to be admitted yet -- or the event was about a
        // connection that is already established. Either way the waiter stays
        // parked and the channel keeps watching.
        job().session.reset();
        job().error = {};
        arm();
        return;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}
} // namespace Foundation::NBIO
