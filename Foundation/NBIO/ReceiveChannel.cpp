#include "ReceiveChannel.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <optional>
#include <span>
#include <spdlog/spdlog.h>

#include <Foundation/Core/Socket.hpp>
#include <cassert>
#include <stdexcept>
#include <utility>

#include <Foundation/Async/Task.hpp>

namespace Foundation::NBIO
{
// The awaiter lives here: it touches the channel's queue, so it operates on the
// concrete (simplex) channel type.
namespace
{
// How many receives one operation may cover, as in the other channels.
constexpr std::size_t kMaximumBatch = 64;

class ReceiveAwaiter
{
  public:
    ReceiveAwaiter(ReceiveChannel &channel, std::span<char> buffer) : channel_(channel), buffer_(buffer)
    {
    }

    ReceiveAwaiter(const ReceiveAwaiter &) = delete;
    ReceiveAwaiter &operator=(const ReceiveAwaiter &) = delete;

    // No cancellation hook. The parked Coroutine keeps this frame's control block
    // alive, and the channel owns the waiter until it answers it, so a stale
    // registration is impossible rather than detected.

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
        static_assert(std::is_base_of_v<Foundation::Async::Promise, PromiseType>,
                      "ReceiveAwaiter requires a promise derived from Foundation::Async::Promise");
        channel_.submit(buffer_, result_, Foundation::Async::Coroutine::from_handle(handle));
    }

    Foundation::Core::ReceiveResult await_resume() noexcept
    {
        return result_;
    }

  private:
    ReceiveChannel &channel_;
    std::span<char> buffer_;
    // Where this receive's outcome lands when the readv that covers it comes back.
    Foundation::Core::ReceiveResult result_{};
};
} // namespace

ReceiveChannel::ReceiveChannel(Foundation::Core::Socket &socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kReceive, socket.native_handle(), multiplexer, scheduler), socket_(socket)
{
    if (!socket.is_valid())
    {
        throw std::logic_error("invalid socket");
    }
    socket_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

ReceiveChannel::~ReceiveChannel() noexcept
{
    multiplexer_.delete_channel(this);

    // Whatever is still queued has nowhere to land now: every waiting frame gets an
    // outcome and a wake-up rather than being left parked for a completion that
    // cannot come.
    for (PendingReceive &pending : pending_)
    {
        fail(pending);
    }
    pending_.clear();
    vectors_.clear();
    armed_ = 0;
}

void ReceiveChannel::submit(std::span<char> buffer, Foundation::Core::ReceiveResult &result, Async::Coroutine waiter)
{
    pending_.push_back(PendingReceive{.buffer = buffer, .result = &result, .waiter = std::move(waiter)});
    if (armed_ == 0)
    {
        arm_batch();
    }
}

void ReceiveChannel::arm_batch()
{
    const std::size_t count = std::min(pending_.size(), kMaximumBatch);
    vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        PendingReceive &receive = pending_[index];
        vectors_[index] = ::iovec{.iov_base = receive.buffer.data(), .iov_len = receive.buffer.size()};
    }
    message_ = ::msghdr{.msg_iov = vectors_.data(), .msg_iovlen = count};
    armed_ = count;
    arm();
}

void ReceiveChannel::refresh_vectors()
{
    vectors_.resize(armed_);
    for (std::size_t index = 0; index < armed_; ++index)
    {
        PendingReceive &receive = pending_[index];
        vectors_[index] = ::iovec{.iov_base = receive.buffer.data(), .iov_len = receive.buffer.size()};
    }
    message_.msg_iov = vectors_.data();
    message_.msg_iovlen = armed_;
}

void ReceiveChannel::complete(std::ptrdiff_t result) noexcept
{
    if (armed_ == 0)
    {
        return;
    }

    if (result < 0)
    {
        // Not ready is not a failure: the receives keep their places and the
        // channel is armed again for when data arrives.
        const int error = -static_cast<int>(result);
        if (error == EAGAIN || error == EWOULDBLOCK)
        {
            return;
        }

        const std::error_code failure{error, std::system_category()};
        for (std::size_t index = 0; index < armed_; ++index)
        {
            PendingReceive &receive = pending_[index];
            if (receive.result != nullptr)
            {
                receive.result->status = Foundation::Core::ReceiveStatus::kError;
                receive.result->error_code = failure;
            }
        }
        return;
    }

    if (result == 0)
    {
        // The peer closed, and it closed for every receive waiting: a stream that
        // has ended has ended for all of them.
        for (std::size_t index = 0; index < armed_; ++index)
        {
            PendingReceive &receive = pending_[index];
            if (receive.result != nullptr)
            {
                receive.result->status = Foundation::Core::ReceiveStatus::kPeerClosed;
                receive.result->bytes_transferred = 0;
            }
        }
        return;
    }

    // A readv fills the buffers in order and stops when the socket has no more
    // data: what it reached is answered, what it did not is still waiting for the
    // next batch -- the socket is a stream, and a short read is not the end of it.
    std::size_t remaining = static_cast<std::size_t>(result);
    for (std::size_t index = 0; index < armed_; ++index)
    {
        PendingReceive &receive = pending_[index];
        if (remaining == 0)
        {
            break;
        }

        const std::size_t taken = std::min(remaining, receive.buffer.size());
        if (receive.result != nullptr)
        {
            receive.result->status = Foundation::Core::ReceiveStatus::kDone;
            receive.result->bytes_transferred = taken;
        }
        receive.buffer = {};
        remaining -= taken;
    }
}

void ReceiveChannel::flush() noexcept
{
    if (armed_ == 0)
    {
        return;
    }

    std::ptrdiff_t result = 0;
    for (;;)
    {
        result = ::readv(native_handle(), vectors_.data(), static_cast<int>(armed_));
        if (result < 0 && errno == EINTR)
        {
            continue;
        }
        break;
    }

    if (result < 0)
    {
        complete(-errno);
        return;
    }

    complete(result);
}

void ReceiveChannel::fail(PendingReceive &pending) noexcept
{
    if (pending.result != nullptr)
    {
        *pending.result = {.status = Foundation::Core::ReceiveStatus::kError,
                           .bytes_transferred = 0,
                           .error_code = std::make_error_code(std::errc::operation_canceled)};
    }
    if (pending.waiter)
    {
        scheduler_.submit(std::move(pending.waiter));
    }
}

void ReceiveChannel::handle_event()
{
    if (handler_) [[likely]]
    {
        handler_(this);
    }

    // Wake every receive that has an answer: they were given to the kernel
    // together, so one readv can have filled several of them.
    while (armed_ > 0 && pending_.front().result != nullptr &&
           pending_.front().result->status != Foundation::Core::ReceiveStatus::kPending)
    {
        PendingReceive finished = std::move(pending_.front());
        pending_.pop_front();
        --armed_;
        if (finished.waiter)
        {
            scheduler_.submit(std::move(finished.waiter));
        }
    }

    if (armed_ > 0)
    {
        refresh_vectors();
        arm();
        return;
    }

    if (!pending_.empty())
    {
        arm_batch();
    }
}

Foundation::NBIO::Task<std::optional<std::size_t>> ReceiveChannel::receive(std::span<char> buffer)
{
    auto result = co_await ReceiveAwaiter{*this, buffer};
    std::optional<std::size_t> ret{};
    if (result.status == Core::ReceiveStatus::kError)
    {
        error_code_ = std::move(result.error_code);
    }
    else
    {
        ret = result.bytes_transferred;
    }
    co_return ret;
}
} // namespace Foundation::NBIO
