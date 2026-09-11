#pragma once

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <coroutine>

#include "Channel.hpp"
#include "Multiplexer.hpp"
#include "Scheduler.hpp"
#include "Task.hpp"

namespace Foundation::Async
{
struct ReceiveJob
{
    Foundation::Core::Buffer *buffer{nullptr};
    Foundation::Core::ReceiveResult result{};
};
// Simplex channel dedicated to receiving: one job, one waiter, interested only
// in the "readable" event.
class ReceiveChannel : public Channel
{
  public:
    explicit ReceiveChannel(Foundation::Core::Socket &socket, Multiplexer &multiplexer, Scheduler &scheduler);
    ~ReceiveChannel() noexcept override;

    Task<Foundation::Core::ReceiveResult> receive(Foundation::Core::Buffer &buffer);

    void on_event() override;

    void set_waiter(std::coroutine_handle<> co) noexcept
    {
        waiter_ = co;
    }
    std::coroutine_handle<> get_waiter() const noexcept
    {
        return waiter_;
    }

    ReceiveJob &job() noexcept
    {
        return job_;
    }
    const ReceiveJob &job() const noexcept
    {
        return job_;
    }
    Foundation::Core::Socket &socket() noexcept
    {
        return socket_;
    }
    const Foundation::Core::Socket &socket() const noexcept
    {
        return socket_;
    }

  private:
    Foundation::Core::Socket &socket_;
    ReceiveJob job_;
    std::coroutine_handle<> waiter_;
};
// The awaiter lives here: it touches the channel's job and waiter, so it
// operates on the concrete (simplex) channel type.
namespace detail
{
class ReceiveAwaiter
{
  public:
    ReceiveAwaiter(ReceiveChannel *channel, Foundation::Core::Buffer &buffer) : channel_(channel), buffer_(buffer)
    {
    }

    ReceiveAwaiter(const ReceiveAwaiter &) = delete;
    ReceiveAwaiter &operator=(const ReceiveAwaiter &) = delete;

    // cancellation hook (Tokio-style): if this frame is destroyed while still
    // the channel's registered waiter (we suspended and were never resumed),
    // unregister so a later readable event never Submits a dangling handle.
    // On the normal path OnEvent already cleared waiter_, so this no-ops.
    ~ReceiveAwaiter()
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

    // Templated on the concrete promise type: coroutine_handle has no
    // derived-to-base conversion, so the compiler-passed
    // coroutine_handle<promise_type> cannot bind to coroutine_handle<Promise>.
    // The base Promise is reached through a reference instead (references do
    // support derived-to-base).
    template <typename PromiseType> void await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
    {
        static_assert(std::is_base_of_v<Promise, PromiseType>,
                      "ReceiveAwaiter requires a promise derived from Promise");
        handle_ = handle;
        channel_->set_waiter(handle);
        ReceiveJob job{.buffer = &buffer_,
                       .result = {.status = Foundation::Core::ReceiveStatus::kPending, .bytes_transferred = 0, .error_code = {}}};
        std::memcpy(&channel_->job(), &job, sizeof(ReceiveJob));
        // Ask the multiplexer to deliver the "readable" event for us.
        channel_->arm();
    }

    Foundation::Core::ReceiveResult await_resume() noexcept
    {
        const ReceiveJob &job = channel_->job();
        assert(job.result.status != Foundation::Core::ReceiveStatus::kPending);
        return job.result;
    }

  private:
    ReceiveChannel *channel_;
    ::Foundation::Core::Buffer &buffer_;
    std::coroutine_handle<> handle_{};
};
} // namespace detail
} // namespace Foundation::Async
