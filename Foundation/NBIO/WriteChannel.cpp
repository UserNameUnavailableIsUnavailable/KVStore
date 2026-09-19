#include "WriteChannel.hpp"
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/File.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "FileStream.hpp"

#include <algorithm>
#include <cerrno>
#include <optional>
#include <span>
#include <system_error>
#include <utility>

#include <unistd.h>

namespace Foundation::NBIO
{
namespace
{
// How many writes one operation may cover. The kernel takes at most IOV_MAX
// entries in a vector call, and beyond a point a larger batch is not a better
// batch: the writes behind it go in the next operation, which follows immediately
// because they are already queued.
constexpr std::size_t kMaximumBatch = 64;
} // namespace

class WriteAwaiter
{
  public:
    WriteAwaiter(WriteChannel &channel, std::span<const char> buffer) : channel_(channel), buffer_(buffer)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType> bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        // The channel hands this write over when the file is free and queues it
        // otherwise. Handing over is what makes it a submission: without it the
        // outcome slot is filled in and the coroutine suspends, but nothing ever
        // asks the backend to write, so the await never completes.
        channel_.prepare(buffer_, result_, Async::Coroutine::from_handle(handle));
        return true;
    }

    Foundation::Core::WriteResult await_resume() const noexcept
    {
        return result_;
    }

  private:
    WriteChannel &channel_;
    std::span<const char> buffer_;
    // Where this write's outcome is left when its turn comes. It lives in the
    // frame that is waiting, and the channel fills it before it schedules that
    // frame: reading it out of the channel's job instead would read whatever the
    // write behind it had put there by then.
    Foundation::Core::WriteResult result_{};
};

WriteChannel::WriteChannel(FileStream &file_stream, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    :
    Channel(Foundation::NBIO::ChannelType::kWrite, file_stream.native_handle(), multiplexer, scheduler),
    file_(file_stream)
{
    multiplexer_.add_channel(this);
}

WriteChannel::~WriteChannel() noexcept
{
    multiplexer_.delete_channel(this);

    for (PendingWrite &pending : pending_)
    {
        drop(pending);
    }
    pending_.clear();
    vectors_.clear();
    submitted_ = 0;
}

void WriteChannel::prepare(std::span<const char> buffer, Foundation::Core::WriteResult &result, Foundation::Async::Coroutine waiter)
{
    result = {.status = Foundation::Core::WriteStatus::kPending, .bytes_transferred = 0, .error_code = {}};
    pending_.emplace_back(buffer, result, std::move(waiter));
    refresh_arming();
}

void WriteChannel::refresh_arming() noexcept
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

std::size_t WriteChannel::count_prepared() noexcept
{
    if (submitted_ != 0 || pending_.empty())
    {
        // The writes of one operation follow one another in the file, so what
        // follows has to wait for it to come back.
        return 0;
    }

    // The offsets come from the file's own cursor: nothing is outstanding, so
    // every byte written before these has landed and these follow one another.
    // The offset and the iovec are recorded now, in queue order, because that is
    // the order the kernel takes them in and the array is what the completion is
    // walked against to decide how many writes it finished.
    std::uint64_t offset = file_.write_offset();
    const std::size_t count = std::min(pending_.size(), kMaximumBatch);
    vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        PendingWrite &write = pending_[index];
        write.set_offset(offset);
        offset += write.buffer().size();
        const std::span<const char> &buffer = write.buffer();
        vectors_[index] = ::iovec{.iov_base = const_cast<char *>(buffer.data()), .iov_len = buffer.size()};
    }
    submitted_ = count;
    return count;
}

void WriteChannel::complete_tasks(std::ptrdiff_t result) noexcept
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
            // The backend refused the operation, and the writes were one
            // operation, so they fail together: the ones behind the front would
            // land past bytes that never made it, and a log with a hole in it is
            // worse than one that says it could not write.
            const std::error_code failure{error, std::system_category()};
            for (std::size_t index = 0; index < submitted_; ++index)
            {
                pending_[index].result().status = Foundation::Core::WriteStatus::kError;
                pending_[index].result().error_code = failure;
            }
        }
        // Not ready leaves them waiting and an error answers them; either way the
        // operation is over and what it covered keeps its place in the queue.
        submitted_ = 0;
        return;
    }

    const auto written = static_cast<std::size_t>(result);
    std::size_t remaining = written;
    for (std::size_t index = 0; index < submitted_; ++index)
    {
        PendingWrite &write = pending_[index];
        const std::size_t size = write.buffer().size();
        const std::size_t taken = std::min(remaining, size);
        write.result().bytes_transferred += taken;

        if (taken == size)
        {
            write.buffer() = {};
            write.result().status = Foundation::Core::WriteStatus::kDone;
        }
        else
        {
            // The operation stopped inside this write: what is left of it keeps
            // its place in the file, and the writes behind it are untouched.
            write.buffer() = write.buffer().subspan(taken);
            write.advance_offset(taken);
        }

        remaining -= taken;
        if (taken != size)
        {
            break;
        }
    }

    if (written != 0)
    {
        // The file's cursor moves with what the backend took, so the next batch
        // starts where this one stopped.
        file_.advance_write_offset(written);
    }
    submitted_ = 0;
}

void WriteChannel::retire_answered() noexcept
{
    while (!pending_.empty() && pending_.front().result().status != Foundation::Core::WriteStatus::kPending)
    {
        PendingWrite finished = std::move(pending_.front());
        pending_.pop_front();
        if (finished.waiter())
        {
            scheduler_.submit(std::move(finished.waiter()));
        }
    }
}

void WriteChannel::drop(PendingWrite &pending) noexcept
{
    pending.result() = {.status = Foundation::Core::WriteStatus::kError,
                        .bytes_transferred = 0,
                        .error_code = std::make_error_code(std::errc::operation_canceled)};
    if (pending.waiter())
    {
        scheduler_.submit(std::move(pending.waiter()));
    }
}

Foundation::NBIO::Task<std::optional<std::size_t>> WriteChannel::write(std::span<const char> buffer)
{
    auto result = co_await WriteAwaiter{*this, buffer};
    std::optional<std::size_t> ret{};
    if (result.error_code)
    {
        error_code_ = std::move(result.error_code);
    }
    if (result.status != Core::WriteStatus::kError)
    {
        ret = result.bytes_transferred;
    }
    co_return ret;
}

void WriteChannel::handle_completion()
{
    // Wake every write that is whole: they were given to the backend together, so
    // one operation can have finished more than one of them.
    retire_answered();
    refresh_arming();
}
} // namespace Foundation::NBIO
