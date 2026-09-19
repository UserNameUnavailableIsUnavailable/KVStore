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

    // Whatever is still queued has nowhere to land now: every waiting frame gets an
    // outcome and a wake-up rather than being left parked for a completion that
    // cannot come. A write that already has its verdict keeps it -- only the wake-up
    // is still owed to it.
    for (PendingWrite &pending : prepared_jobs_)
    {
        drop(pending);
    }
    for (PendingWrite &pending : submitted_jobs_)
    {
        drop(pending);
    }
    for (PendingWrite &done : completed_jobs_)
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
}

std::span<const ::iovec> WriteChannel::submit_jobs()
{
    if (!submitted_jobs_.empty())
    {
        // A batch is out there already. The backend asks again once it is
        // concluded, so there is nothing to hand over now. The writes of one batch
        // follow one another in the file, so they cannot be split up.
        return {};
    }
    if (prepared_jobs_.empty())
    {
        return {}; // nothing to submit
    }

    // The offsets come from the file's own cursor: nothing is outstanding, so every
    // byte written before these has landed and these follow one another. The offset
    // and the iovec are recorded now, in queue order, because that is the order the
    // kernel takes them in and the array its one answer is spread over.
    std::uint64_t offset = file_.write_offset();
    const std::size_t count = std::min(prepared_jobs_.size(), kMaximumBatch);
    vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        submitted_jobs_.push_back(std::move(prepared_jobs_.front()));
        prepared_jobs_.pop_front();

        PendingWrite &write = submitted_jobs_.back();
        write.set_offset(offset);
        offset += write.buffer().size();
        const std::span<const char> &buffer = write.buffer();
        vectors_[index] = ::iovec{.iov_base = const_cast<char *>(buffer.data()), .iov_len = buffer.size()};
    }

    return std::span<const ::iovec>{vectors_.data(), count};
}

void WriteChannel::advance_job(std::ptrdiff_t result) noexcept
{
    // One operation's outcome, spread over the jobs it covered: the file takes them
    // in the order they were queued, so the front of the batch takes the bytes and a
    // write the operation stopped inside keeps its place, holding what is left of
    // it at the offset the kernel stopped at.
    if (result < 0)
    {
        const int error = -static_cast<int>(result);
        if (error == EAGAIN || error == EWOULDBLOCK)
        {
            // Not ready is not an answer: every job in the batch keeps waiting.
            return;
        }

        // The backend refused the operation, and the writes were one operation, so
        // they fail together: the ones behind the front would land past bytes that
        // never made it, and a log with a hole in it is worse than one that says it
        // could not write.
        const std::error_code failure{error, std::system_category()};
        while (!submitted_jobs_.empty())
        {
            PendingWrite write = std::move(submitted_jobs_.front());
            submitted_jobs_.pop_front();
            retire(std::move(write), Foundation::Core::WriteStatus::kError, failure);
        }
        return;
    }

    const auto written = static_cast<std::size_t>(result);
    std::size_t remaining = written;
    while (remaining > 0 && !submitted_jobs_.empty())
    {
        PendingWrite write = std::move(submitted_jobs_.front());
        submitted_jobs_.pop_front();
        const std::size_t size = write.buffer().size();
        const std::size_t taken = std::min(remaining, size);
        write.result().bytes_transferred += taken;

        if (taken == size)
        {
            retire(std::move(write), Foundation::Core::WriteStatus::kDone, {});
        }
        else
        {
            // The operation stopped inside this write: what is left of it keeps its
            // place in the file, and it goes back to the front of the queue.
            write.buffer() = write.buffer().subspan(taken);
            write.advance_offset(taken);
            submitted_jobs_.push_front(std::move(write));
        }
        remaining -= taken;
    }

    if (written != 0)
    {
        // The file's cursor moves with what the backend took, so the next batch
        // starts where this one stopped.
        file_.advance_write_offset(written);
    }
}

void WriteChannel::complete_jobs() noexcept
{
    // The operation is over. Whatever it did not finish goes back to the front of
    // the queue, in its original order, so the next operation starts where this one
    // stopped.
    prepared_jobs_.insert(prepared_jobs_.begin(), std::make_move_iterator(submitted_jobs_.begin()),
                          std::make_move_iterator(submitted_jobs_.end()));
    submitted_jobs_.clear();
    vectors_.clear();
}

void WriteChannel::prepare(std::span<const char> buffer, Foundation::Core::WriteResult &result, Foundation::Async::Coroutine waiter)
{
    result = {.status = Foundation::Core::WriteStatus::kPending, .bytes_transferred = 0, .error_code = {}};
    prepared_jobs_.emplace_back(buffer, result, std::move(waiter));

    // Armed from the moment there is something to wait for: an armed channel is one
    // the backend looks at, and it stays armed until the queue runs dry.
    arm();
}

void WriteChannel::retire(PendingWrite job, Foundation::Core::WriteStatus status, std::error_code error) noexcept
{
    // Only the verdict is written here: how much of this write went out is already
    // in its outcome slot, counted by advance_job().
    job.result().status = status;
    job.result().error_code = std::move(error);
    job.buffer() = {};
    completed_jobs_.push_back(std::move(job));
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
    // Every write that has a verdict wakes here: they were given to the backend
    // together, so one operation can have finished more than one of them. This runs
    // once the batch's completions have all been advanced, so a resumed write cannot
    // queue work behind a job that is still being answered.
    for (PendingWrite &job : completed_jobs_)
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
} // namespace Foundation::NBIO
