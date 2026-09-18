#include "WriteChannel.hpp"
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/File.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "FileStream.hpp"

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

struct WriteAwaiter
{
    WriteChannel &channel;
    std::span<const char> buffer;

    // Where this write's outcome is left when its turn comes. It lives in the
    // frame that is waiting, and the channel fills it before it schedules that
    // frame: reading it out of the channel's job instead would read whatever the
    // write behind it had put there by then.
    Foundation::Core::WriteResult result{};

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        // The channel arms this write when the file is free and queues it
        // otherwise. Arming is what hands the write to the multiplexer: without it
        // the job is filled in and the coroutine suspends, but nothing ever
        // submits the write -- so the await never completes.
        channel.submit(buffer, result, Async::Coroutine::from_handle(handle));
        return true;
    }

    Foundation::Core::WriteResult await_resume() const noexcept
    {
        return result;
    }
};
} // namespace

WriteChannel::WriteChannel(FileStream &file_stream, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kWrite, file_stream.native_handle(), multiplexer, scheduler), file_(file_stream)
{
    multiplexer_.add_channel(this);
}

WriteChannel::~WriteChannel() noexcept
{
    multiplexer_.delete_channel(this);

    // Whatever is still queued has nowhere to land now. Every waiting frame is
    // given an outcome and woken rather than left holding a coroutine that
    // nothing will ever resume: a dropped coroutine is a session that never
    // answers again, and a control block the scheduler never gets to reclaim.
    for (PendingWrite &pending : pending_)
    {
        fail(pending);
    }
    pending_.clear();
    vectors_.clear();
    armed_ = 0;
}

void WriteChannel::submit(std::span<const char> buffer, Foundation::Core::WriteResult &result, Async::Coroutine waiter)
{
    pending_.push_back(PendingWrite{.buffer = buffer, .result = &result, .waiter = std::move(waiter)});
    if (armed_ == 0)
    {
        // Nothing is with the kernel, so everything queued can go together --
        // which is what the queue is for: one operation for all of them instead
        // of one apiece.
        arm_batch();
    }
}

void WriteChannel::arm_batch()
{
    // The offsets come from the file's own cursor: nothing is armed, so every byte
    // written before these has landed and these follow one another.
    std::uint64_t offset = file_.write_offset();
    const std::size_t count = std::min(pending_.size(), kMaximumBatch);
    vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        PendingWrite &job = pending_[index];
        job.offset = offset;
        offset += job.buffer.size();
        vectors_[index] = ::iovec{.iov_base = const_cast<char *>(job.buffer.data()), .iov_len = job.buffer.size()};
    }
    armed_ = count;
    arm();
}

void WriteChannel::refresh_vectors()
{
    vectors_.resize(armed_);
    for (std::size_t index = 0; index < armed_; ++index)
    {
        PendingWrite &job = pending_[index];
        vectors_[index] = ::iovec{.iov_base = const_cast<char *>(job.buffer.data()), .iov_len = job.buffer.size()};
    }
}

void WriteChannel::complete(std::ptrdiff_t result) noexcept
{
    if (armed_ == 0)
    {
        return;
    }

    if (result < 0)
    {
        // Not ready is not a failure: nothing was written and the batch stays
        // armed for when the file says it is ready.
        const int error = -static_cast<int>(result);
        if (error == EAGAIN || error == EWOULDBLOCK)
        {
            return;
        }

        // The kernel refused the operation, and the writes were one operation, so
        // they fail together: the ones behind the front would land past bytes that
        // never made it, and a log with a hole in it is worse than one that says
        // it could not write.
        const std::error_code failure{error, std::system_category()};
        for (std::size_t index = 0; index < armed_; ++index)
        {
            PendingWrite &job = pending_[index];
            if (job.result != nullptr)
            {
                job.result->status = Foundation::Core::WriteStatus::kError;
                job.result->error_code = failure;
            }
        }
        return;
    }

    const auto written = static_cast<std::size_t>(result);
    std::size_t remaining = written;
    for (std::size_t index = 0; index < armed_; ++index)
    {
        PendingWrite &job = pending_[index];
        const std::size_t size = job.buffer.size();
        const std::size_t taken = std::min(remaining, size);
        if (job.result != nullptr)
        {
            job.result->bytes_transferred += taken;
        }

        if (taken == size)
        {
            job.buffer = {};
            if (job.result != nullptr)
            {
                job.result->status = Foundation::Core::WriteStatus::kDone;
            }
        }
        else
        {
            // The operation stopped inside this write: what is left of it keeps its
            // place in the file, and the writes behind it are untouched.
            job.buffer = job.buffer.subspan(taken);
            job.offset += taken;
        }

        remaining -= taken;
        if (taken != size)
        {
            break;
        }
    }

    if (written != 0)
    {
        // The file's cursor moves with what the kernel took, so the next batch
        // starts where this one stopped.
        file_.advance_write_offset(written);
    }
}

void WriteChannel::flush() noexcept
{
    const std::size_t count = armed_;
    if (count == 0)
    {
        return;
    }

    std::ptrdiff_t result = 0;
    for (;;)
    {
        result = ::pwritev(native_handle(), vectors_.data(), static_cast<int>(count), static_cast<off_t>(front_offset()));
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

void WriteChannel::fail(PendingWrite &pending) noexcept
{
    if (pending.result != nullptr)
    {
        *pending.result = {.status = Foundation::Core::WriteStatus::kError,
                           .bytes_transferred = 0,
                           .error_code = std::make_error_code(std::errc::operation_canceled)};
    }
    if (pending.waiter)
    {
        scheduler_.submit(std::move(pending.waiter));
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

void WriteChannel::handle_event()
{
    if (handler_) [[likely]]
    {
        // A readiness multiplexer does the writing here, through flush(); a
        // completion multiplexer has already handed the byte count to complete()
        // before calling here.
        handler_(this);
    }

    // Wake every write that is whole: they were given to the kernel together, so
    // one operation can have finished more than one of them.
    while (armed_ > 0 && pending_.front().result != nullptr &&
           pending_.front().result->status != Foundation::Core::WriteStatus::kPending)
    {
        PendingWrite finished = std::move(pending_.front());
        pending_.pop_front();
        --armed_;
        if (finished.waiter)
        {
            scheduler_.submit(std::move(finished.waiter));
        }
    }

    if (armed_ > 0)
    {
        // The operation stopped inside the write at the front of the queue, so
        // that write keeps the channel for what is left of it, with the writes
        // still armed behind it.
        refresh_vectors();
        arm();
        return;
    }

    if (!pending_.empty())
    {
        // Everything the operation covered is done and more has arrived since:
        // those writes go together, the way the last lot did.
        arm_batch();
    }
}
} // namespace Foundation::NBIO
