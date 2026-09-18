#include "SendChannel.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <Foundation/Core/Socket.hpp>
#include <cassert>
#include <cerrno>
#include <optional>
#include <span>
#include <utility>

namespace Foundation::NBIO
{
namespace
{
// How many sends one operation may cover, as in the other channels.
constexpr std::size_t kMaximumBatch = 64;

// The awaiter lives here: it touches the channel's queue, so it operates on the
// concrete (simplex) channel type.
class SendAwaiter
{
  public:
    SendAwaiter(SendChannel &channel, std::span<const char> buffer) : channel_(channel), buffer_(buffer)
    {
    }

    SendAwaiter(const SendAwaiter &) = delete;
    SendAwaiter &operator=(const SendAwaiter &) = delete;

    // No cancellation hook. The parked Coroutine keeps this frame's control block
    // alive, and the channel owns the waiter until it answers it, so a stale
    // registration is impossible rather than detected.

    bool await_ready() noexcept
    {
        const std::ptrdiff_t taken = channel_.send_now(buffer_);
        if (taken < 0)
        {
            // A batch is in flight: this send takes its place in the queue.
            return false;
        }
        if (static_cast<std::size_t>(taken) == buffer_.size())
        {
            // The kernel took all of it, so there is nothing to wait for and no
            // reason to touch the multiplexer.
            result_.status = Foundation::Core::SendStatus::kDone;
            result_.bytes_transferred = buffer_.size();
            return true;
        }

        // Part of it went out. What went is already counted -- the channel adds
        // the rest to this same result -- and only the remainder is parked.
        result_.bytes_transferred = static_cast<std::size_t>(taken);
        remaining_ = buffer_.subspan(static_cast<std::size_t>(taken));
        return false;
    }

    template <typename PromiseType> void await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
    {
        channel_.submit(remaining_.empty() ? buffer_ : remaining_, result_, Foundation::Async::Coroutine::from_handle(handle));
    }

    Foundation::Core::SendResult await_resume() noexcept
    {
        return result_;
    }

  private:
    SendChannel &channel_;
    std::span<const char> buffer_;
    // What is left of the send after await_ready() got part of it out; empty
    // while nothing has been sent.
    std::span<const char> remaining_{};
    // Where this send's outcome lands when the sendmsg that covers it comes back.
    Foundation::Core::SendResult result_{};
};
} // namespace

SendChannel::SendChannel(Foundation::Core::Socket &socket, Foundation::Async::Scheduler &scheduler, Foundation::NBIO::Multiplexer &multiplexer)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kSend, socket.native_handle(), multiplexer, scheduler), socket_(socket)
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

    // Whatever is still queued has nowhere to go now: every waiting frame gets an
    // outcome and a wake-up rather than being left parked for a completion that
    // cannot come.
    for (PendingSend &pending : pending_)
    {
        fail(pending);
    }
    pending_.clear();
    vectors_.clear();
    message_ = {};
    armed_ = 0;
}

void SendChannel::submit(std::span<const char> buffer, Foundation::Core::SendResult &result, Async::Coroutine waiter)
{
    pending_.push_back(PendingSend{.buffer = buffer, .result = &result, .waiter = std::move(waiter)});
    if (armed_ == 0)
    {
        arm_batch();
    }
}

void SendChannel::arm_batch()
{
    const std::size_t count = std::min(pending_.size(), kMaximumBatch);
    vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        PendingSend &send = pending_[index];
        vectors_[index] = ::iovec{.iov_base = const_cast<char *>(send.buffer.data()), .iov_len = send.buffer.size()};
    }
    message_ = ::msghdr{.msg_iov = vectors_.data(), .msg_iovlen = count};
    armed_ = count;
    arm();
}

void SendChannel::refresh_vectors()
{
    vectors_.resize(armed_);
    for (std::size_t index = 0; index < armed_; ++index)
    {
        PendingSend &send = pending_[index];
        vectors_[index] = ::iovec{.iov_base = const_cast<char *>(send.buffer.data()), .iov_len = send.buffer.size()};
    }
    message_.msg_iov = vectors_.data();
    message_.msg_iovlen = armed_;
}

void SendChannel::complete(std::ptrdiff_t result) noexcept
{
    if (armed_ == 0)
    {
        return;
    }

    if (result < 0)
    {
        // Not ready is not a failure: the sends keep their places and the channel
        // is armed again for when the socket can take them.
        const int error = -static_cast<int>(result);
        if (error == EAGAIN || error == EWOULDBLOCK)
        {
            return;
        }

        // A stream that cannot be written to is one stream: every send in the
        // operation fails, because the ones behind it would follow the same
        // connection.
        const std::error_code failure{error, std::system_category()};
        for (std::size_t index = 0; index < armed_; ++index)
        {
            PendingSend &send = pending_[index];
            if (send.result != nullptr)
            {
                send.result->status = Foundation::Core::SendStatus::kError;
                send.result->error_code = failure;
            }
        }
        return;
    }

    std::size_t remaining = static_cast<std::size_t>(result);
    if (remaining == 0 && !pending_.front().buffer.empty())
    {
        // Nothing was taken from bytes that were there to send. Asking again would
        // spin forever, so it is reported instead.
        PendingSend &front = pending_.front();
        if (front.result != nullptr)
        {
            front.result->status = Foundation::Core::SendStatus::kError;
            front.result->error_code = std::make_error_code(std::errc::io_error);
        }
        return;
    }

    for (std::size_t index = 0; index < armed_; ++index)
    {
        PendingSend &send = pending_[index];
        const std::size_t size = send.buffer.size();
        const std::size_t taken = std::min(remaining, size);
        if (send.result != nullptr)
        {
            send.result->bytes_transferred += taken;
        }

        if (taken == size)
        {
            send.buffer = {};
            if (send.result != nullptr)
            {
                send.result->status = Foundation::Core::SendStatus::kDone;
            }
        }
        else
        {
            // The operation stopped inside this send: what is left of it goes
            // first next time, and the sends behind it keep their order.
            send.buffer = send.buffer.subspan(taken);
        }

        remaining -= taken;
        if (taken != size)
        {
            break;
        }
    }
}

void SendChannel::flush() noexcept
{
    if (armed_ == 0)
    {
        return;
    }

    ::msghdr message{.msg_iov = vectors_.data(), .msg_iovlen = armed_};
    std::ptrdiff_t result = 0;
    for (;;)
    {
        result = ::sendmsg(native_handle(), &message, MSG_NOSIGNAL);
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

void SendChannel::fail(PendingSend &pending) noexcept
{
    if (pending.result != nullptr)
    {
        *pending.result = {.status = Foundation::Core::SendStatus::kError,
                           .bytes_transferred = 0,
                           .error_code = std::make_error_code(std::errc::operation_canceled)};
    }
    if (pending.waiter)
    {
        scheduler_.submit(std::move(pending.waiter));
    }
}

void SendChannel::handle_event()
{
    if (handler_) [[likely]]
    {
        handler_(this);
    }

    // Wake every send that has an answer, in the order the stream took them: one
    // sendmsg can have finished several.
    while (armed_ > 0 && pending_.front().result != nullptr &&
           pending_.front().result->status != Foundation::Core::SendStatus::kPending)
    {
        PendingSend finished = std::move(pending_.front());
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

Foundation::NBIO::Task<std::optional<std::size_t>> SendChannel::send(std::span<const char> buffer)
{
    auto result = co_await SendAwaiter{*this, buffer};
    std::optional<std::size_t> ret{};
    if (result.status == Core::SendStatus::kError)
    {
        error_code_ = std::move(result.error_code);
    }
    else
    {
        ret = result.bytes_transferred;
    }
    co_return ret;
}

std::ptrdiff_t SendChannel::send_now(std::span<const char> buffer) noexcept
{
    // Order is what a stream is, so this only applies when nothing is in flight:
    // a send that arrives while a batch is armed takes its place behind it.
    if (armed_ != 0 || !pending_.empty())
    {
        return -1;
    }

    ::iovec vector{.iov_base = const_cast<char *>(buffer.data()), .iov_len = buffer.size()};
    ::msghdr message{.msg_iov = &vector, .msg_iovlen = 1};
    std::ptrdiff_t sent = 0;
    do
    {
        sent = ::sendmsg(native_handle(), &message, MSG_NOSIGNAL);
    } while (sent < 0 && errno == EINTR);

    if (sent < 0)
    {
        // Not ready is not a failure: the caller parks and is woken when the
        // socket can take the bytes, which is also where a real error is
        // reported.
        return 0;
    }
    return sent;
}

} // namespace Foundation::NBIO
