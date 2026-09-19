#include "ReadChannel.hpp"
#include <Foundation/Core/File.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "FileStream.hpp"

#include <algorithm>
#include <cerrno>
#include <optional>
#include <system_error>
#include <utility>

#include <unistd.h>

namespace Foundation::NBIO
{
namespace
{
// How many reads one operation may cover, as in the write channel: the kernel
// takes at most IOV_MAX entries, and the reads behind the batch go in the next one.
constexpr std::size_t kMaximumBatch = 64;
} // namespace

class ReadAwaiter
{
  public:
    ReadAwaiter(ReadChannel &channel, std::span<char> buffer) : channel_(channel), buffer_(buffer)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType> bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        channel_.prepare(buffer_, result_, Async::Coroutine::from_handle(handle));
        return true;
    }

    Foundation::Core::ReadResult await_resume() const noexcept
    {
        return result_;
    }

  private:
    ReadChannel &channel_;
    std::span<char> buffer_;
    // Where this read's outcome is left when its turn comes: it belongs to the
    // frame that is waiting, which is how several reads of one batch each get
    // their own answer.
    Foundation::Core::ReadResult result_{};
};

ReadChannel::ReadChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRead, static_cast<std::uintptr_t>(file.native_handle()), multiplexer, scheduler),
      file_(file)
{
    multiplexer_.add_channel(this);
}

ReadChannel::~ReadChannel() noexcept
{
    multiplexer_.delete_channel(this);

    // Whatever is still queued has nowhere to land now: every waiting frame gets an
    // outcome and a wake-up rather than being left parked for a completion that
    // cannot come.
    for (PendingRead &pending : pending_)
    {
        drop(pending);
    }
    pending_.clear();
    vectors_.clear();
    submitted_ = 0;
}

void ReadChannel::prepare(std::span<char> buffer, Foundation::Core::ReadResult &result, Foundation::Async::Coroutine waiter)
{
    result = {.status = Foundation::Core::ReadStatus::kPending, .bytes_transferred = 0, .error_code = {}};
    pending_.emplace_back(buffer, result, std::move(waiter));
    refresh_arming();
}

void ReadChannel::refresh_arming() noexcept
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

std::size_t ReadChannel::count_prepared() noexcept
{
    if (submitted_ != 0 || pending_.empty())
    {
        // The reads of one operation cover consecutive stretches of the file, so
        // what follows has to wait for it to come back.
        return 0;
    }

    // The offsets come from the file's own cursor, which is where the next read
    // starts, and the reads cover consecutive stretches of it. Both the offset and
    // the iovec are recorded now, in queue order, because that is the order the
    // kernel fills them in and the array is what the completion is walked against.
    std::uint64_t offset = file_.read_offset();
    const std::size_t count = std::min(pending_.size(), kMaximumBatch);
    vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        PendingRead &read = pending_[index];
        read.set_offset(offset);
        offset += read.buffer().size();
        const std::span<char> &buffer = read.buffer();
        vectors_[index] = ::iovec{.iov_base = buffer.data(), .iov_len = buffer.size()};
    }
    submitted_ = count;
    return count;
}

void ReadChannel::complete_tasks(std::ptrdiff_t result) noexcept
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
            // The backend refused the operation: every read in it fails together,
            // the same way they were made together.
            const std::error_code failure{error, std::system_category()};
            for (std::size_t index = 0; index < submitted_; ++index)
            {
                pending_[index].result().status = Foundation::Core::ReadStatus::kError;
                pending_[index].result().error_code = failure;
            }
        }
        // Not ready leaves them waiting and an error answers them; either way the
        // operation is over and what it covered keeps its place in the queue.
        submitted_ = 0;
        return;
    }

    std::size_t remaining = static_cast<std::size_t>(result);
    for (std::size_t index = 0; index < submitted_; ++index)
    {
        PendingRead &read = pending_[index];
        if (remaining == 0)
        {
            // The backend stopped before this read, and a short read is a read: it
            // is the backend saying it has nothing more for now. For a file that
            // means the end, which is also what the reads behind it get, because
            // they would read at or past that end.
            read.result().status = Foundation::Core::ReadStatus::kEndOfFile;
            read.result().bytes_transferred = 0;
            read.buffer() = {};
            continue;
        }

        const std::size_t taken = std::min(remaining, read.buffer().size());
        read.result().status = Foundation::Core::ReadStatus::kDone;
        read.result().bytes_transferred = taken;
        read.buffer() = {};
        remaining -= taken;
    }

    if (result != 0)
    {
        // The file's cursor moves with what was read, so the next batch starts
        // where this one stopped.
        file_.advance_read_offset(static_cast<std::size_t>(result));
    }
    submitted_ = 0;
}

void ReadChannel::retire_answered() noexcept
{
    while (!pending_.empty() && pending_.front().result().status != Foundation::Core::ReadStatus::kPending)
    {
        PendingRead finished = std::move(pending_.front());
        pending_.pop_front();
        if (finished.waiter())
        {
            scheduler_.submit(std::move(finished.waiter()));
        }
    }
}

void ReadChannel::drop(PendingRead &pending) noexcept
{
    pending.result() = {.status = Foundation::Core::ReadStatus::kError,
                        .bytes_transferred = 0,
                        .error_code = std::make_error_code(std::errc::operation_canceled)};
    if (pending.waiter())
    {
        scheduler_.submit(std::move(pending.waiter()));
    }
}

Foundation::NBIO::Task<std::optional<std::size_t>> ReadChannel::read(std::span<char> buffer)
{
    std::optional<std::size_t> ret{};
    auto result = co_await ReadAwaiter{*this, buffer};
    if (result.status == Core::ReadStatus::kError)
    {
        error_code_ = result.error_code;
    }
    else
    {
        ret = result.bytes_transferred;
    }
    co_return ret;
}

void ReadChannel::handle_completion()
{
    // Wake every read that has an answer: they were given to the backend together,
    // so one operation can have finished several of them.
    retire_answered();
    refresh_arming();
}
} // namespace Foundation::NBIO
