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
    // cannot come. A read that already has its answer keeps it -- only the wake-up
    // is still owed to it.
    for (PendingRead &pending : prepared_jobs_)
    {
        drop(pending);
    }
    for (PendingRead &pending : submitted_jobs_)
    {
        drop(pending);
    }
    for (PendingRead &done : completed_jobs_)
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

std::span<const ::iovec> ReadChannel::submit_jobs()
{
    if (!submitted_jobs_.empty())
    {
        // A batch is out there already. The backend asks again once it is
        // concluded, so there is nothing to hand over now. The reads of one batch
        // cover consecutive stretches of the file, so they cannot be split up.
        return {};
    }
    if (prepared_jobs_.empty())
    {
        return {}; // nothing to submit
    }

    // The offsets come from the file's own cursor, which is where the next read
    // starts, and the reads cover consecutive stretches of it. Both the offset and
    // the iovec are recorded now, in queue order, because that is the order the
    // kernel fills them in and the array the outcome is spread over.
    std::uint64_t offset = file_.read_offset();
    const std::size_t count = std::min(prepared_jobs_.size(), kMaximumBatch);
    vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        submitted_jobs_.push_back(std::move(prepared_jobs_.front()));
        prepared_jobs_.pop_front();

        PendingRead &read = submitted_jobs_.back();
        read.set_offset(offset);
        offset += read.buffer().size();
        const std::span<char> &buffer = read.buffer();
        vectors_[index] = ::iovec{.iov_base = buffer.data(), .iov_len = buffer.size()};
    }

    return std::span<const ::iovec>{vectors_.data(), count};
}

void ReadChannel::advance_job(std::ptrdiff_t result) noexcept
{
    // One operation's outcome, spread over the jobs it covered: the kernel fills
    // the buffers in the order they were queued, so the front of the batch takes
    // the bytes.
    if (result < 0)
    {
        const int error = -static_cast<int>(result);
        if (error == EAGAIN || error == EWOULDBLOCK)
        {
            // Not ready is not an answer: every job in the batch keeps waiting.
            return;
        }

        // The backend refused the operation: every read in it fails together, the
        // same way they were made together.
        const std::error_code failure{error, std::system_category()};
        while (!submitted_jobs_.empty())
        {
            PendingRead read = std::move(submitted_jobs_.front());
            submitted_jobs_.pop_front();
            retire(std::move(read), {.status = Foundation::Core::ReadStatus::kError, .bytes_transferred = 0, .error_code = failure});
        }
        return;
    }

    const auto read_bytes = static_cast<std::size_t>(result);
    std::size_t remaining = read_bytes;
    while (!submitted_jobs_.empty())
    {
        PendingRead read = std::move(submitted_jobs_.front());
        submitted_jobs_.pop_front();

        if (remaining == 0)
        {
            // The backend stopped before this read, and a short read is a read: it
            // is the backend saying it has nothing more for now. For a file that
            // means the end -- and it is the end for the reads behind it too,
            // because they would read at or past where it stopped.
            retire(std::move(read), {.status = Foundation::Core::ReadStatus::kEndOfFile, .bytes_transferred = 0, .error_code = {}});
            continue;
        }

        const std::size_t taken = std::min(remaining, read.buffer().size());
        retire(std::move(read), {.status = Foundation::Core::ReadStatus::kDone, .bytes_transferred = taken, .error_code = {}});
        remaining -= taken;
    }

    if (read_bytes != 0)
    {
        // The file's cursor moves with what was read, so the next batch starts
        // where this one stopped.
        file_.advance_read_offset(read_bytes);
    }
}

void ReadChannel::complete_jobs() noexcept
{
    // The operation is over. What it did not answer goes back to the front of the
    // queue, in its original order, so the next operation starts where this one
    // stopped.
    prepared_jobs_.insert(prepared_jobs_.begin(), std::make_move_iterator(submitted_jobs_.begin()),
                          std::make_move_iterator(submitted_jobs_.end()));
    submitted_jobs_.clear();
    vectors_.clear();
}

void ReadChannel::prepare(std::span<char> buffer, Foundation::Core::ReadResult &result, Foundation::Async::Coroutine waiter)
{
    result = {.status = Foundation::Core::ReadStatus::kPending, .bytes_transferred = 0, .error_code = {}};
    prepared_jobs_.emplace_back(buffer, result, std::move(waiter));

    // Armed from the moment there is something to wait for: an armed channel is one
    // the backend looks at, and it stays armed until the queue runs dry.
    arm();
}

void ReadChannel::retire(PendingRead job, Foundation::Core::ReadResult result) noexcept
{
    job.result() = result;
    job.buffer() = {};
    completed_jobs_.push_back(std::move(job));
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
    // Every read that has an answer wakes here, in the order the file was read:
    // they were given to the backend together, so one operation can have finished
    // several of them. This runs once the batch's completions have all been
    // advanced, so a resumed read cannot queue work behind a job that is still
    // being answered.
    for (PendingRead &job : completed_jobs_)
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
