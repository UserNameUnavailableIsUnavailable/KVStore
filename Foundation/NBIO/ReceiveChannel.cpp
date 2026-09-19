#include "ReceiveChannel.hpp"
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/ReadChannel.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <algorithm>
#include <iterator>
#include <optional>
#include <span>
#include <spdlog/spdlog.h>

#include <Foundation/Core/Socket.hpp>
#include <cassert>
#include <cerrno>
#include <stdexcept>
#include <sys/socket.h>
#include <utility>

#include <Foundation/Async/Task.hpp>

namespace Foundation::NBIO
{
namespace
{
// How many receives one operation may cover, as in the other channels.
constexpr std::size_t kMaximumBatch = 64;
} // namespace

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
        channel_.prepare(buffer_, result_, Async::Coroutine::from_handle(handle));
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

ReceiveChannel::ReceiveChannel(Foundation::Core::Socket &socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kReceive, static_cast<std::uintptr_t>(socket.native_handle()), multiplexer, scheduler), socket_(socket)
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
    // cannot come. A job that already has its answer keeps it -- only the wake-up
    // is still owed to it.
    for (PendingReceive &pending : prepared_jobs_)
    {
        drop(pending);
    }
    for (PendingReceive &pending : submitted_jobs_)
    {
        drop(pending);
    }
    for (PendingReceive &done : completed_jobs_)
    {
        if (done.waiter())
        {
            scheduler_.submit(std::move(done.waiter()));
        }
    }
    prepared_jobs_.clear();
    submitted_jobs_.clear();
    completed_jobs_.clear();
    io_vectors_.clear();
}

::msghdr *ReceiveChannel::submit_jobs()
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

    // The batch is the prepared prefix, in queue order. The iovecs say how much
    // each receive offered, which is both the order the kernel fills them in and
    // the array its one answer is spread over.
    const std::size_t count = std::min(prepared_jobs_.size(), kMaximumBatch);
    io_vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        submitted_jobs_.push_back(std::move(prepared_jobs_.front()));
        prepared_jobs_.pop_front();

        const std::span<char> &buffer = submitted_jobs_.back().buffer();
        io_vectors_[index] = ::iovec{.iov_base = buffer.data(), .iov_len = buffer.size()};
    }

    message_header_ = ::msghdr{.msg_iov = io_vectors_.data(), .msg_iovlen = count};
    return &message_header_;
}

void ReceiveChannel::advance_job(std::ptrdiff_t result) noexcept
{
    // One operation's outcome, spread over the jobs it covered: the kernel fills
    // the buffers in the order they were queued, so the front of the batch takes
    // the bytes and whatever it did not reach keeps its place for complete_jobs().
    if (result < 0)
    {
        const int error = -static_cast<int>(result);
        if (error == EAGAIN || error == EWOULDBLOCK)
        {
            // Not ready is not an answer: every job in the batch keeps waiting.
            return;
        }

        // A socket that cannot be read from is one socket: every receive in the
        // operation fails, because the ones behind it would follow the same
        // connection.
        const std::error_code failure{error, std::system_category()};
        while (!submitted_jobs_.empty())
        {
            PendingReceive receive = std::move(submitted_jobs_.front());
            submitted_jobs_.pop_front();
            retire(std::move(receive),
                   {.status = Foundation::Core::ReceiveStatus::kError, .bytes_received = 0, .error_code = failure});
        }
        return;
    }

    if (result == 0)
    {
        // The peer closed, and it closed for every receive waiting: a stream that
        // has ended has ended for all of them.
        while (!submitted_jobs_.empty())
        {
            PendingReceive receive = std::move(submitted_jobs_.front());
            submitted_jobs_.pop_front();
            retire(std::move(receive),
                   {.status = Foundation::Core::ReceiveStatus::kPeerClosed, .bytes_received = 0, .error_code = {}});
        }
        return;
    }

    std::size_t remaining = static_cast<std::size_t>(result);
    while (remaining > 0 && !submitted_jobs_.empty())
    {
        PendingReceive receive = std::move(submitted_jobs_.front());
        submitted_jobs_.pop_front();
        const std::size_t taken = std::min(remaining, receive.buffer().size());
        retire(std::move(receive),
               {.status = Foundation::Core::ReceiveStatus::kDone, .bytes_received = taken, .error_code = {}});
        remaining -= taken;
    }
}

void ReceiveChannel::complete_jobs() noexcept
{
    // The operation is over. What it did not answer goes back to the front of the
    // queue, in its original order, so the next operation starts where this one
    // stopped.
    prepared_jobs_.insert(prepared_jobs_.begin(), std::make_move_iterator(submitted_jobs_.begin()),
                          std::make_move_iterator(submitted_jobs_.end()));
    submitted_jobs_.clear();
    io_vectors_.clear();
    message_header_ = ::msghdr{};
}

void ReceiveChannel::prepare(std::span<char> buffer, Foundation::Core::ReceiveResult &result, Foundation::Async::Coroutine waiter)
{
    result = {.status = Foundation::Core::ReceiveStatus::kPending, .bytes_received = 0, .error_code = {}};
    prepared_jobs_.emplace_back(buffer, result, std::move(waiter));

    // Armed from the moment there is something to wait for: an armed channel is one
    // the backend looks at, and it stays armed until the queue runs dry.
    arm();
}

void ReceiveChannel::retire(PendingReceive job, Foundation::Core::ReceiveResult result) noexcept
{
    job.result() = result;
    job.buffer() = {};
    completed_jobs_.push_back(std::move(job));
}

void ReceiveChannel::drop(PendingReceive &pending) noexcept
{
    pending.result() = {.status = Foundation::Core::ReceiveStatus::kError,
                        .bytes_received = 0,
                        .error_code = std::make_error_code(std::errc::operation_canceled)};
    if (pending.waiter())
    {
        scheduler_.submit(std::move(pending.waiter()));
    }
}

void ReceiveChannel::handle_completion()
{
    // Every job that has an answer wakes here, in the order the stream filled them.
    // This runs once every completion of the batch has been advanced, so a resumed
    // receive cannot queue work behind a job that is still being answered.
    for (PendingReceive &job : completed_jobs_)
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
        ret = result.bytes_received;
    }
    co_return ret;
}
} // namespace Foundation::NBIO
