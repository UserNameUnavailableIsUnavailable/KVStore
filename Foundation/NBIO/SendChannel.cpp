#include "SendChannel.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <Foundation/Core/Socket.hpp>
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <optional>
#include <span>
#include <system_error>
#include <utility>

namespace Foundation::NBIO
{
namespace
{
// How many sends one operation may cover, as in the other channels.
constexpr std::size_t kMaximumBatch = 64;

} // namespace

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
            result_.bytes_sent = buffer_.size();
            return true;
        }

        // Part of it went out. What went is already counted -- the channel adds
        // the rest to this same result -- and only the remainder is parked.
        result_.bytes_sent = static_cast<std::size_t>(taken);
        remaining_ = buffer_.subspan(static_cast<std::size_t>(taken));
        return false;
    }

    template <typename PromiseType> void await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
    {
        channel_.prepare(remaining_.empty() ? buffer_ : remaining_, result_, Foundation::Async::Coroutine::from_handle(handle));
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
        drop(pending);
    }
    pending_.clear();
    vectors_.clear();
    message_header_ = {};
    submitted_ = 0;
}

void SendChannel::prepare(std::span<const char> buffer, Foundation::Core::SendResult &result, Foundation::Async::Coroutine waiter)
{
    // The outcome slot already holds whatever the fast path sent, so it is not
    // reset here: only the bytes still to go are queued.
    pending_.emplace_back(buffer, result, std::move(waiter));
    refresh_arming();
}

void SendChannel::refresh_arming() noexcept
{
    const bool can_hand_over = has_prepared();

    if (submits_immediately())
    {
        // Readiness is the submission: the work goes over now, and the channel
        // stays armed while there is anything to wait for.
        if (can_hand_over)
        {
            count_prepared();
        }

        if (submitted_ == 0 && pending_.empty())
        {
            disarm();
            return;
        }
        arm();
        return;
    }

    // A completion backend runs the submission phase itself, so the channel is
    // armed exactly while it has something for that phase to hand over.
    if (can_hand_over)
    {
        arm();
        return;
    }
    disarm();
}

std::size_t SendChannel::count_prepared() noexcept
{
    if (submitted_ != 0 || pending_.empty())
    {
        // A stream is written in order, so one operation is with the backend at a
        // time: the sends behind it wait their turn.
        return 0;
    }

    // The bytes are described as iovecs in queue order, which is the order the
    // stream has to keep, and the array is what the completion is walked against
    // to decide how many sends it finished.
    const std::size_t count = std::min(pending_.size(), kMaximumBatch);
    vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        const std::span<const char> &buffer = pending_[index].buffer();
        vectors_[index] = ::iovec{.iov_base = const_cast<char *>(buffer.data()), .iov_len = buffer.size()};
    }
    message_header_ = ::msghdr{.msg_iov = vectors_.data(), .msg_iovlen = count};
    submitted_ = count;
    return count;
}

void SendChannel::complete_tasks(std::ptrdiff_t result) noexcept
{
    if (submitted_ == 0)
    {
        return;
    }

    if (result < 0)
    {
        const int error = -static_cast<int>(result);
        if (error != EAGAIN && error != EWOULDBLOCK)
        {
            // A stream that cannot be written to is one stream: every send in the
            // operation fails, because the ones behind it would follow the same
            // connection.
            const std::error_code failure{error, std::system_category()};
            for (std::size_t index = 0; index < submitted_; ++index)
            {
                pending_[index].result().status = Foundation::Core::SendStatus::kError;
                pending_[index].result().error_code = failure;
            }
        }
        // Not ready leaves them waiting and an error answers them; either way the
        // operation is over and what it covered keeps its place in the queue.
        submitted_ = 0;
        return;
    }

    std::size_t remaining = static_cast<std::size_t>(result);
    if (remaining == 0 && !pending_.front().buffer().empty())
    {
        // Nothing was taken from bytes that were there to send. Asking again would
        // spin forever, so it is reported instead.
        pending_.front().result().status = Foundation::Core::SendStatus::kError;
        pending_.front().result().error_code = std::make_error_code(std::errc::io_error);
        submitted_ = 0;
        return;
    }

    for (std::size_t index = 0; index < submitted_; ++index)
    {
        PendingSend &send = pending_[index];
        const std::size_t size = send.buffer().size();
        const std::size_t taken = std::min(remaining, size);
        send.result().bytes_sent += taken;

        if (taken == size)
        {
            send.buffer() = {};
            send.result().status = Foundation::Core::SendStatus::kDone;
        }
        else
        {
            // The operation stopped inside this send: what is left of it goes
            // first next time, and the sends behind it keep their order.
            send.buffer() = send.buffer().subspan(taken);
        }

        remaining -= taken;
        if (taken != size)
        {
            break;
        }
    }
    submitted_ = 0;
}

void SendChannel::retire_answered() noexcept
{
    // The finished sends are at the front, in the order the stream took them; the
    // one the operation stopped inside keeps its place, with what is left of it.
    while (!pending_.empty() && pending_.front().result().status != Foundation::Core::SendStatus::kPending)
    {
        PendingSend finished = std::move(pending_.front());
        pending_.pop_front();
        if (finished.waiter())
        {
            scheduler_.submit(std::move(finished.waiter()));
        }
    }
}

void SendChannel::drop(PendingSend &pending) noexcept
{
    pending.result() = {.status = Foundation::Core::SendStatus::kError,
                        .bytes_sent = 0,
                        .error_code = std::make_error_code(std::errc::operation_canceled)};
    if (pending.waiter())
    {
        scheduler_.submit(std::move(pending.waiter()));
    }
}

void SendChannel::handle_completion()
{
    // Wake every send that has an answer, in the order the stream took them: one
    // sendmsg can have finished several.
    retire_answered();
    refresh_arming();
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
        ret = result.bytes_sent;
    }
    co_return ret;
}

std::ptrdiff_t SendChannel::send_now(std::span<const char> buffer) noexcept
{
    // Order is what a stream is, so this only applies when nothing is in flight:
    // a send that arrives while an operation is outstanding takes its place behind
    // it rather than jumping ahead of the bytes already promised to the socket.
    if (submitted_ != 0 || !pending_.empty())
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
