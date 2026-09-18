#include "ReadChannel.hpp"
#include <Foundation/Core/File.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "FileStream.hpp"

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

struct ReadAwaiter
{
    ReadChannel &channel;
    std::span<char> buffer;

    // Where this read's outcome is left when its turn comes: it belongs to the
    // frame that is waiting, which is how several reads of one batch each get
    // their own answer.
    Foundation::Core::ReadResult result{};

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        channel.submit(buffer, result, Async::Coroutine::from_handle(handle));
        return true;
    }

    Foundation::Core::ReadResult await_resume() const noexcept
    {
        return result;
    }
};
} // namespace

ReadChannel::ReadChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRead, file.native_handle(), multiplexer, scheduler), file_(file)
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
        fail(pending);
    }
    pending_.clear();
    vectors_.clear();
    armed_ = 0;
}

void ReadChannel::submit(std::span<char> buffer, Foundation::Core::ReadResult &result, Async::Coroutine waiter)
{
    pending_.push_back(PendingRead{.buffer = buffer, .result = &result, .waiter = std::move(waiter)});
    if (armed_ == 0)
    {
        arm_batch();
    }
}

void ReadChannel::arm_batch()
{
    // The offsets come from the file's own cursor, which is where the next read
    // starts, and the reads cover consecutive stretches of it.
    std::uint64_t offset = file_.read_offset();
    const std::size_t count = std::min(pending_.size(), kMaximumBatch);
    vectors_.resize(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        PendingRead &read = pending_[index];
        read.offset = offset;
        offset += read.buffer.size();
        vectors_[index] = ::iovec{.iov_base = read.buffer.data(), .iov_len = read.buffer.size()};
    }
    armed_ = count;
    arm();
}

void ReadChannel::refresh_vectors()
{
    vectors_.resize(armed_);
    for (std::size_t index = 0; index < armed_; ++index)
    {
        PendingRead &read = pending_[index];
        vectors_[index] = ::iovec{.iov_base = read.buffer.data(), .iov_len = read.buffer.size()};
    }
}

void ReadChannel::complete(std::ptrdiff_t result) noexcept
{
    if (armed_ == 0)
    {
        return;
    }

    if (result < 0)
    {
        // Not ready is not a failure: the batch stays armed and the kernel is
        // asked again.
        const int error = -static_cast<int>(result);
        if (error == EAGAIN || error == EWOULDBLOCK)
        {
            return;
        }

        // The kernel refused the operation: every read in it fails together, the
        // same way they were made together.
        const std::error_code failure{error, std::system_category()};
        for (std::size_t index = 0; index < armed_; ++index)
        {
            PendingRead &read = pending_[index];
            if (read.result != nullptr)
            {
                read.result->status = Foundation::Core::ReadStatus::kError;
                read.result->error_code = failure;
            }
        }
        return;
    }

    std::size_t remaining = static_cast<std::size_t>(result);
    for (std::size_t index = 0; index < armed_; ++index)
    {
        PendingRead &read = pending_[index];
        if (remaining == 0)
        {
            // The kernel stopped before this read, and a short read is a read: it
            // is the kernel saying it has nothing more for now. For a file that
            // means the end, which is also what the reads behind it get, because
            // they would read at or past that end.
            if (read.result != nullptr)
            {
                read.result->status = Foundation::Core::ReadStatus::kEndOfFile;
                read.result->bytes_transferred = 0;
            }
            read.buffer = {};
            continue;
        }

        const std::size_t taken = std::min(remaining, read.buffer.size());
        if (read.result != nullptr)
        {
            read.result->status = Foundation::Core::ReadStatus::kDone;
            read.result->bytes_transferred = taken;
        }
        read.buffer = {};
        remaining -= taken;
    }

    if (result != 0)
    {
        // The file's cursor moves with what was read, so the next batch starts
        // where this one stopped.
        file_.advance_read_offset(static_cast<std::size_t>(result));
    }
}

void ReadChannel::flush() noexcept
{
    const std::size_t count = armed_;
    if (count == 0)
    {
        return;
    }

    std::ptrdiff_t result = 0;
    for (;;)
    {
        result = ::preadv(native_handle(), vectors_.data(), static_cast<int>(count), static_cast<off_t>(front_offset()));
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

void ReadChannel::fail(PendingRead &pending) noexcept
{
    if (pending.result != nullptr)
    {
        *pending.result = {.status = Foundation::Core::ReadStatus::kError,
                           .bytes_transferred = 0,
                           .error_code = std::make_error_code(std::errc::operation_canceled)};
    }
    if (pending.waiter)
    {
        scheduler_.submit(std::move(pending.waiter));
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

void ReadChannel::handle_event()
{
    if (handler_) [[likely]]
    {
        handler_(this);
    }

    // Wake every read that has an answer: they were given to the kernel together,
    // so one operation can have finished several of them.
    while (armed_ > 0 && pending_.front().result != nullptr &&
           pending_.front().result->status != Foundation::Core::ReadStatus::kPending)
    {
        PendingRead finished = std::move(pending_.front());
        pending_.pop_front();
        --armed_;
        if (finished.waiter)
        {
            scheduler_.submit(std::move(finished.waiter));
        }
    }

    if (armed_ > 0)
    {
        // The operation ended without reaching these, so they stay armed and go
        // back to the kernel as the batch they are.
        refresh_vectors();
        arm();
        return;
    }

    if (!pending_.empty())
    {
        arm_batch();
    }
}
} // namespace Foundation::NBIO
