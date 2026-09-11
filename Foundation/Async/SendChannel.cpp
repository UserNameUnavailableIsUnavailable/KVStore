#include "SendChannel.hpp"

#include <Foundation/Socket.hpp>
#include <cassert>
#include <utility>

#include "spdlog/spdlog.h"

namespace Foundation::Async
{
namespace
{
class SendAwaiter
{
  public:
    SendAwaiter(SendChannel *channel, ::Foundation::Buffer &buffer) : channel_(channel), buffer_(buffer)
    {
    }

    SendAwaiter(const SendAwaiter &) = delete;
    SendAwaiter &operator=(const SendAwaiter &) = delete;

    // cancellation hook: unregister if the frame is destroyed while still the
    // channel's waiter (suspended, never resumed). No-op on the normal path.
    ~SendAwaiter()
    {
        if (handle_ && channel_->get_waiter() == handle_)
        {
            channel_->disarm();
            channel_->set_waiter({});
        }
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) noexcept
    {
        handle_ = handle;
        channel_->set_waiter(handle);
        SendJob job{.buffer = &buffer_,
                    .result = {.status = SendStatus::kPending, .bytes_transferred = 0, .error_code = {}}};
        std::memcpy(&channel_->job(), &job, sizeof(SendJob));
        // Ask the multiplexer to deliver the "writable" event for us.
        channel_->arm();
    }

    SendResult await_resume() noexcept
    {
        const SendJob &job = channel_->job();
        assert(job.result.status != SendStatus::kPending);
        return job.result;
    }

  private:
    SendChannel *channel_;
    ::Foundation::Buffer &buffer_;
    std::coroutine_handle<> handle_{};
};
} // namespace

SendChannel::SendChannel(Foundation::Socket &socket, Scheduler &scheduler, Multiplexer &multiplexer)
    : Channel(ChannelType::kSend, socket.native_handle(), multiplexer, scheduler), socket_(socket)
{
    if (!socket_.is_valid())
    {
        throw std::logic_error("socket is invalid");
    }
    socket_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

SendChannel::~SendChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void SendChannel::on_event()
{
    if (!waiter_)
    {
        spdlog::warn("Event is triggered but no one is waiting for it, ignored.");
        return;
    }

    if (handler_ != nullptr)
    {
        handler_(this);
    }

    if (job_.result.status == SendStatus::kPending)
    {
        // Not finished (EAGAIN, or partially written): stay armed.
        arm();
        return;
    }

    armed_ = false;
    auto waiter = std::exchange(waiter_, {});
    if (waiter && !waiter.done())
    {
        scheduler_.submit(waiter);
    }
}

Task<SendResult> SendChannel::send(::Foundation::Buffer &buffer)
{
    auto result = co_await SendAwaiter(this, buffer);
    co_return std::move(result);
}
} // namespace Foundation::Async
