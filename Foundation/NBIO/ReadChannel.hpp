#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/File.hpp>
#include <cstdint>
#include <deque>
#include <optional>
#include <system_error>
#include <sys/uio.h>
#include <vector>


namespace Foundation::NBIO
{
class FileStream;

class ReadAwaiter;

// One read waiting its turn. The buffer and the slot the outcome is written into
// belong to the frame that is parked, which is alive for exactly as long as this
// entry is queued, so the entry points at them rather than owning them.
class PendingRead
{
  public:
    PendingRead(std::span<char> buffer, Foundation::Core::ReadResult &result, Foundation::Async::Coroutine waiter) noexcept
        : buffer_(buffer), result_(&result), waiter_(std::move(waiter))
    {
    }

    std::span<char> &buffer() noexcept
    {
        return buffer_;
    }
    const std::span<char> &buffer() const noexcept
    {
        return buffer_;
    }

    // Where this read lands in the file. Filled when the read is handed over as
    // part of an operation, because the reads of one batch cover consecutive
    // stretches of the file.
    std::uint64_t offset() const noexcept
    {
        return offset_;
    }
    void set_offset(std::uint64_t offset) noexcept
    {
        offset_ = offset;
    }

    Foundation::Core::ReadResult &result() noexcept
    {
        return *result_;
    }
    const Foundation::Core::ReadResult &result() const noexcept
    {
        return *result_;
    }

    Foundation::Async::Coroutine &waiter() noexcept
    {
        return waiter_;
    }

  private:
    std::span<char> buffer_;
    std::uint64_t offset_{0};
    Foundation::Core::ReadResult *result_{nullptr};
    Foundation::Async::Coroutine waiter_;
};

class ReadChannel final : public Foundation::NBIO::Channel
{
  public:
    ReadChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~ReadChannel() noexcept;

    Foundation::NBIO::Task<std::optional<std::size_t>> read(std::span<char> buffer);

    // The batch protocol (see Channel.hpp). One operation is one preadv over the
    // whole prepared prefix: the reads cover consecutive stretches of the file, so
    // where the first of them lands is where the batch starts.
    std::span<const ::iovec> submit_jobs();
    void advance_job(std::ptrdiff_t result) noexcept;
    void complete_jobs() noexcept;
    void handle_completion();

    // Where the batch that is out there starts in the file.
    std::uint64_t batch_offset() const noexcept
    {
        return submitted_jobs_.empty() ? 0 : submitted_jobs_.front().offset();
    }

    FileStream &file_stream() noexcept
    {
        return file_;
    }
    const FileStream &file_stream() const noexcept
    {
        return file_;
    }

    std::error_code last_error() const noexcept
    {
        return error_code_;
    }

  private:
    friend class ReadAwaiter;

    // Queues the read and arms the channel: this is the suspension point, and
    // being armed is what tells the backend to look at the channel.
    void prepare(std::span<char> buffer, Foundation::Core::ReadResult &result, Foundation::Async::Coroutine waiter);

    // Gives one job its answer and moves it to the completed queue.
    void retire(PendingRead job, Foundation::Core::ReadResult result) noexcept;

    void drop(PendingRead &pending) noexcept;

    FileStream &file_;
    // The queue, in the order the reads were awaited. What one operation covers is
    // a prefix of it: those jobs are held in `submitted_jobs_` while the operation
    // is out there, and in `completed_jobs_` once they have an answer.
    std::deque<PendingRead> prepared_jobs_;
    std::deque<PendingRead> submitted_jobs_;
    std::deque<PendingRead> completed_jobs_;
    std::vector<::iovec> vectors_;
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
