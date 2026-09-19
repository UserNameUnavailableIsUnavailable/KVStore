#include "ReceiveChannel.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <algorithm>
#include <optional>
#include <span>
#include <spdlog/spdlog.h>

#include <Foundation/Core/Socket.hpp>
#include <cassert>
#include <cerrno>
#include <stdexcept>
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
        static_assert(std::is_base_of_v<Foundation::Async::Promise, PromiseType>,
                      "ReceiveAwaiter requires a promise derived from Foundation::Async::Promise");
        channel_.prepare(buffer_, result_, Foundation::Async::Coroutine::from_handle(handle));
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
    // cannot come.
    for (PendingReceive &pending : pending_)
    {
        drop(pending);
    }
    pending_.clear();
    io_vectors_.clear();
    submitted_ = 0;
}

void ReceiveChannel::prepare(std::span<char> buffer, Foundation::Core::ReceiveResult &result, Foundation::Async::Coroutine waiter)
{
    result = {.status = Foundation::Core::ReceiveStatus::kPending, .bytes_received = 0, .error_code = {}};
    pending_.emplace_back(buffer, result, std::move(waiter));
    refresh_arming();
}

void ReceiveChannel::refresh_arming() noexcept
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

std::size_t ReceiveChannel::count_prepared() noexcept
{
    if (submitted_ != 0 || pending_.empty())
    {
        // A stream is read in order, so one operation is with the backend at a
        // time: the receives behind it wait their turn.
        return 0;
    }

    // The buffers are pushed as iovecs now, in queue order: that is the order the
    // kernel fills them in, and the array is what the completion is walked against
    // to decide how many receives it answered.
    const std::size_t count = std::min(pending_.size(), kMaximumBatch);
    io_vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        const std::span<char> &buffer = pending_[index].buffer();
        io_vectors_[index] = ::iovec{.iov_base = buffer.data(), .iov_len = buffer.size()};
    }
    message_header_ = ::msghdr{.msg_iov = io_vectors_.data(), .msg_iovlen = count};
    submitted_ = count;
    return count;
}

void ReceiveChannel::complete_tasks(std::ptrdiff_t result) noexcept
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
            // A socket that cannot be read from is one socket: every receive in the
            // operation fails, because the ones behind it would follow the same
            // connection.
            const std::error_code failure{error, std::system_category()};
            for (std::size_t index = 0; index < submitted_; ++index)
            {
                pending_[index].result().status = Foundation::Core::ReceiveStatus::kError;
                pending_[index].result().error_code = failure;
            }
        }
        // Not ready leaves them waiting and an error answers them; either way the
        // operation is over and what it covered keeps its place in the queue.
        submitted_ = 0;
        return;
    }

    if (result == 0)
    {
        // The peer closed, and it closed for every receive waiting: a stream that
        // has ended has ended for all of them.
        for (std::size_t index = 0; index < submitted_; ++index)
        {
            pending_[index].result().status = Foundation::Core::ReceiveStatus::kPeerClosed;
            pending_[index].result().bytes_received = 0;
        }
        submitted_ = 0;
        return;
    }

    // The kernel filled the buffers in order and stopped when the socket had no
    // more data: what it reached is answered, what it did not is still waiting for
    // the next batch -- the socket is a stream, and a short read is not the end.
    std::size_t remaining = static_cast<std::size_t>(result);
    for (std::size_t index = 0; index < submitted_; ++index)
    {
        if (remaining == 0)
        {
            break;
        }

        PendingReceive &receive = pending_[index];
        const std::size_t taken = std::min(remaining, receive.buffer().size());
        receive.result().status = Foundation::Core::ReceiveStatus::kDone;
        receive.result().bytes_received = taken;
        receive.buffer() = {};
        remaining -= taken;
    }
    submitted_ = 0;
}

void ReceiveChannel::retire_answered() noexcept
{
    // The answered receives are at the front, in the order the stream filled them;
    // the ones that were not reached keep their places and their buffers.
    while (!pending_.empty() && pending_.front().result().status != Foundation::Core::ReceiveStatus::kPending)
    {
        PendingReceive finished = std::move(pending_.front());
        pending_.pop_front();
        if (finished.waiter())
        {
            scheduler_.submit(std::move(finished.waiter()));
        }
    }
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
    retire_answered();
    refresh_arming();
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
