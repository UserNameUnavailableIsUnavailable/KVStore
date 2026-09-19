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

    // Wakes what the operation answered and decides what the backend owes next.
    void handle_completion();

    // Work the backend could take right now: there is something queued and no
    // operation of ours is with the kernel.
    bool has_prepared() const noexcept
    {
        return submitted_ == 0 && !pending_.empty();
    }

    // Hands the prepared prefix over as one operation: fills each read's offset
    // from the file's cursor, builds the iovecs, and answers how many reads it
    // covers.
    std::size_t count_prepared() noexcept;

    // The operation in flight reported its outcome.
    void complete_tasks(std::ptrdiff_t result) noexcept;

    // Is an operation of this channel's with the kernel? The reads of one
    // operation cover consecutive stretches, so there is at most one outstanding.
    bool has_submitted() const noexcept
    {
        return submitted_ != 0;
    }

    std::span<const ::iovec> vectors() const noexcept
    {
        return std::span<const ::iovec>{vectors_.data(), submitted_};
    }

    // Where the first of them lands.
    std::uint64_t front_offset() const noexcept
    {
        return pending_.empty() ? 0 : pending_.front().offset();
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

    void prepare(std::span<char> buffer, Foundation::Core::ReadResult &result, Foundation::Async::Coroutine waiter);

    void refresh_arming() noexcept;

    // Retires the answered reads at the front of the queue, in the order the file
    // was read, and hands their waiters back to the scheduler.
    void retire_answered() noexcept;

    void drop(PendingRead &pending) noexcept;

    FileStream &file_;
    std::deque<PendingRead> pending_;
    // How many of `pending_`, from the front, the kernel has been given.
    std::size_t submitted_{0};
    std::vector<::iovec> vectors_;
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
