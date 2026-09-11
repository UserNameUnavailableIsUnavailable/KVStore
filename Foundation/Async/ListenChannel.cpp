#include "ListenChannel.hpp"

#include <Foundation/Async/Multiplexer.hpp>
#include <Foundation/Socket.hpp>
#include <cassert>
#include <stdexcept>
#include <utility>

#include "Scheduler.hpp"

namespace Foundation::Async
{
ListenChannel::ListenChannel(Foundation::Socket &socket, Multiplexer &multiplexer, Scheduler &scheduler)
    : Channel(ChannelType::kListen, socket.native_handle(), multiplexer, scheduler), socket_(socket)
{
    if (!socket_.is_valid())
    {
        throw std::logic_error("invalid socket");
    }
    socket_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

ListenChannel::~ListenChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

namespace
{
class AcceptAwaiter
{
  public:
    AcceptAwaiter(ListenChannel *channel) : channel_(channel)
    {
    }

    AcceptAwaiter(const AcceptAwaiter &) = delete;
    AcceptAwaiter &operator=(const AcceptAwaiter &) = delete;

    // cancellation hook: unregister if the frame is destroyed while still the
    // channel's waiter (suspended, never resumed). No-op on the normal path.
    ~AcceptAwaiter()
    {
        if (handle_ && channel_->Getwaiter() == handle_)
        {
            channel_->disarm();
            channel_->Setwaiter({});
        }
    }

    bool await_ready() const noexcept
    {
        return false;
    }
    void await_suspend(std::coroutine_handle<> handle)
    {
        handle_ = handle;
        channel_->Setwaiter(handle);
        channel_->job() =
            AcceptJob{.result = {.status = AcceptStatus::kPending, .socket = {}, .address = {}, .error_code = {}}};
        // Ask the multiplexer to deliver the "readable" event for us.
        channel_->arm();
    }

    AcceptResult await_resume() noexcept
    {
        AcceptJob &job = channel_->job();
        assert(job.result.status != AcceptStatus::kPending);
        return std::move(job.result);
    }

  private:
    ListenChannel *channel_;
    std::coroutine_handle<> handle_{};
};
} // namespace

void ListenChannel::on_event()
{
    if (!waiter_)
    {
        return;
    }

    if (handler_ != nullptr)
    {
        handler_(this);
    }

    if (job().result.status == AcceptStatus::kPending)
    {
        // No connection was ready (EAGAIN): stay armed, keep suspended.
        arm();
        return;
    }

    armed_ = false;
    auto waiter = std::exchange(waiter_, nullptr);
    if (waiter && !waiter.done())
    {
        scheduler_.submit(waiter);
    }
}

Task<AcceptResult> ListenChannel::Accept()
{
    auto result = co_await AcceptAwaiter(this);
    co_return std::move(result);
}
} // namespace Foundation::Async
