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
    // cannot come. A send that already has its answer keeps it -- only the wake-up
    // is still owed to it.
    for (PendingSend &pending : prepared_jobs_)
    {
        drop(pending);
    }
    for (PendingSend &pending : submitted_jobs_)
    {
        drop(pending);
    }
    for (PendingSend &done : completed_jobs_)
    {
        if (done.waiter())
        {
            scheduler_.submit(std::move(done.waiter()));
        }
    }
    prepared_jobs_.clear();
    submitted_jobs_.clear();
    completed_jobs_.clear();
    vectors_.clear();
    message_header_ = {};
}

::msghdr *SendChannel::submit_jobs()
{
    if (!submitted_jobs_.empty())
    {
        // A batch is out there already. The backend asks again once it is
        // concluded, so there is nothing to hand over now.
        return nullptr;
    }
    if (prepared_jobs_.empty())
    {
        return nullptr; // nothing to submit
    }

    // The batch is the prepared prefix, in queue order -- which is the order the
    // stream has to keep. The iovecs are what says so to the kernel, and they are
    // the array its one answer is spread over.
    const std::size_t count = std::min(prepared_jobs_.size(), kMaximumBatch);
    vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        submitted_jobs_.push_back(std::move(prepared_jobs_.front()));
        prepared_jobs_.pop_front();

        const std::span<const char> &buffer = submitted_jobs_.back().buffer();
        vectors_[index] = ::iovec{.iov_base = const_cast<char *>(buffer.data()), .iov_len = buffer.size()};
    }

    message_header_ = ::msghdr{.msg_iov = vectors_.data(), .msg_iovlen = count};
    return &message_header_;
}

void SendChannel::advance_job(std::ptrdiff_t result) noexcept
{
    // One operation's outcome, spread over the jobs it covered: the stream takes
    // them in the order they were queued, so the front of the batch takes the bytes
    // and a send the operation stopped inside keeps its place, holding what is left
    // of it.
    if (result < 0)
    {
        const int error = -static_cast<int>(result);
        if (error == EAGAIN || error == EWOULDBLOCK)
        {
            // Not ready is not an answer: every job in the batch keeps waiting.
            return;
        }

        // A stream that cannot be written to is one stream: every send in the
        // operation fails, because the ones behind it would follow the same
        // connection.
        const std::error_code failure{error, std::system_category()};
        while (!submitted_jobs_.empty())
        {
            PendingSend send = std::move(submitted_jobs_.front());
            submitted_jobs_.pop_front();
            retire(std::move(send), Foundation::Core::SendStatus::kError, failure);
        }
        return;
    }

    const auto written = static_cast<std::size_t>(result);
    if (written == 0)
    {
        // Nothing was taken from bytes that were there to send. Asking again would
        // spin forever, so the front is told instead.
        if (!submitted_jobs_.empty() && !submitted_jobs_.front().buffer().empty())
        {
            PendingSend send = std::move(submitted_jobs_.front());
            submitted_jobs_.pop_front();
            send.result().status = Foundation::Core::SendStatus::kError;
            send.result().error_code = std::make_error_code(std::errc::io_error);
            completed_jobs_.push_back(std::move(send));
        }
        return;
    }

    std::size_t remaining = written;
    while (remaining > 0 && !submitted_jobs_.empty())
    {
        PendingSend send = std::move(submitted_jobs_.front());
        submitted_jobs_.pop_front();
        const std::size_t size = send.buffer().size();
        const std::size_t taken = std::min(remaining, size);

        // The outcome slot already holds whatever the fast path sent, so this adds
        // to it rather than replacing it.
        send.result().bytes_sent += taken;

        if (taken == size)
        {
            retire(std::move(send), Foundation::Core::SendStatus::kDone, {});
        }
        else
        {
            // The operation stopped inside this send: what is left of it goes first
            // next time, and it keeps its place at the front of the queue.
            send.buffer() = send.buffer().subspan(taken);
            submitted_jobs_.push_front(std::move(send));
        }
        remaining -= taken;
    }
}

void SendChannel::complete_jobs() noexcept
{
    // The operation is over. Whatever it did not finish goes back to the front of
    // the queue, in its original order, so the next operation starts where this one
    // stopped.
    prepared_jobs_.insert(prepared_jobs_.begin(), std::make_move_iterator(submitted_jobs_.begin()),
                          std::make_move_iterator(submitted_jobs_.end()));
    submitted_jobs_.clear();
    vectors_.clear();
    message_header_ = {};
}

void SendChannel::prepare(std::span<const char> buffer, Foundation::Core::SendResult &result, Foundation::Async::Coroutine waiter)
{
    // The outcome slot already holds whatever the fast path sent, so it is not
    // reset here: only the bytes still to go are queued.
    prepared_jobs_.emplace_back(buffer, result, std::move(waiter));

    // Armed from the moment there is something to wait for: an armed channel is one
    // the backend looks at, and it stays armed until the queue runs dry.
    arm();
}

void SendChannel::retire(PendingSend job, Foundation::Core::SendStatus status, std::error_code error) noexcept
{
    // Only the verdict is written here: how much of this send went out is already
    // in its outcome slot, counted by the fast path or by advance_job().
    job.result().status = status;
    job.result().error_code = std::move(error);
    job.buffer() = {};
    completed_jobs_.push_back(std::move(job));
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
    // Every send that has an answer wakes here, in the order the stream took them:
    // one sendmsg can have finished several. This runs once the batch's completions
    // have all been advanced, so a resumed send cannot queue work behind a job that
    // is still being answered.
    for (PendingSend &job : completed_jobs_)
    {
        if (job.waiter())
        {
            scheduler_.submit(std::move(job.waiter()));
        }
    }
    completed_jobs_.clear();

    // Armed while there is anything left to wait for, disarmed otherwise: the
    // backend submits for an armed channel and leaves a disarmed one alone.
    if (prepared_jobs_.empty())
    {
        disarm();
    }
    else
    {
        arm();
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
        ret = result.bytes_sent;
    }
    co_return ret;
}

std::ptrdiff_t SendChannel::send_now(std::span<const char> buffer) noexcept
{
    // Order is what a stream is, so this only applies when nothing of ours is in
    // flight: a send that arrives while an operation is outstanding -- or while
    // other sends are queued -- takes its place behind them rather than jumping
    // ahead of the bytes already promised to the socket.
    if (!submitted_jobs_.empty() || !prepared_jobs_.empty())
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
