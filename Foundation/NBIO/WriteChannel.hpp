#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/File.hpp>
#include <cstdint>
#include <deque>
#include <system_error>
#include <sys/uio.h>
#include <vector>


namespace Foundation::NBIO
{
class FileStream;

class WriteAwaiter;

// One write waiting for its turn on the channel. The bytes and the slot the
// outcome is written into both belong to the frame that is parked, which is alive
// for exactly as long as the entry is queued -- and the channel writes into that
// slot rather than into a job of its own, because several of these are handed to
// the kernel at once.
class PendingWrite
{
  public:
    PendingWrite(std::span<const char> buffer, Foundation::Core::WriteResult &result, Foundation::Async::Coroutine waiter) noexcept
        : buffer_(buffer), result_(&result), waiter_(std::move(waiter))
    {
    }

    std::span<const char> &buffer() noexcept
    {
        return buffer_;
    }
    const std::span<const char> &buffer() const noexcept
    {
        return buffer_;
    }

    // Where the rest of this write goes in the file. Filled when the write is
    // handed over as part of an operation, and advanced by whatever a short write
    // leaves behind.
    std::uint64_t offset() const noexcept
    {
        return offset_;
    }
    void set_offset(std::uint64_t offset) noexcept
    {
        offset_ = offset;
    }
    void advance_offset(std::uint64_t taken) noexcept
    {
        offset_ += taken;
    }

    Foundation::Core::WriteResult &result() noexcept
    {
        return *result_;
    }
    const Foundation::Core::WriteResult &result() const noexcept
    {
        return *result_;
    }

    Foundation::Async::Coroutine &waiter() noexcept
    {
        return waiter_;
    }

  private:
    std::span<const char> buffer_;
    std::uint64_t offset_{0};
    Foundation::Core::WriteResult *result_{nullptr};
    Foundation::Async::Coroutine waiter_;
};

class WriteChannel final : public Foundation::NBIO::Channel
{
  public:
    WriteChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~WriteChannel() noexcept;

    Foundation::NBIO::Task<std::optional<std::size_t>> write(std::span<const char> buffer);

    // The batch protocol (see Channel.hpp). One operation is one pwritev over the
    // whole prepared prefix: the writes follow one another in the file, so where the
    // first of them lands is where the batch starts.
    std::span<const ::iovec> submit_jobs();
    void advance_job(std::ptrdiff_t result) noexcept;
    void complete_jobs() noexcept;
    void handle_completion();

    // Where the batch that is out there starts in the file.
    std::uint64_t batch_offset() const noexcept
    {
        return submitted_jobs_.empty() ? 0 : submitted_jobs_.front().offset();
    }

    FileStream &file() noexcept
    {
        return file_;
    }
    const FileStream &file() const noexcept
    {
        return file_;
    }

    std::error_code last_error() const noexcept
    {
        return error_code_;
    }

  private:
    friend class WriteAwaiter;

    // Queues the write and arms the channel: this is the suspension point, and
    // being armed is what tells the backend to look at the channel.
    void prepare(std::span<const char> buffer, Foundation::Core::WriteResult &result, Foundation::Async::Coroutine waiter);

    // Gives one job its verdict and moves it to the completed queue. How much of it
    // went out is already counted in its outcome slot by the time this runs.
    void retire(PendingWrite job, Foundation::Core::WriteStatus status, std::error_code error) noexcept;

    // Leaves a waiting frame with an outcome, so that nothing is ever parked for a
    // completion that cannot come. Used when the channel goes away.
    void drop(PendingWrite &pending) noexcept;

    FileStream &file_;
    // The queue, in the order the writes were awaited. What one operation covers is
    // a prefix of it: those jobs are held in `submitted_jobs_` while the operation
    // is out there, and in `completed_jobs_` once they are whole. A write the
    // operation stopped inside stays in `submitted_jobs_`, at the front, holding
    // what is left of its buffer.
    std::deque<PendingWrite> prepared_jobs_;
    std::deque<PendingWrite> submitted_jobs_;
    std::deque<PendingWrite> completed_jobs_;
    std::vector<::iovec> vectors_;
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
