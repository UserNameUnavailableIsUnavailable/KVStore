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

    // Wakes what the operation finished and decides what the backend owes next.
    void handle_completion();

    // Work the backend could take right now: there is something queued and no
    // operation of ours is with the kernel.
    bool has_prepared() const noexcept
    {
        return submitted_ == 0 && !pending_.empty();
    }

    // Hands the prepared prefix over as one operation: fills each write's offset
    // from the file's cursor, builds the iovecs, and answers how many writes it
    // covers.
    std::size_t count_prepared() noexcept;

    // The operation in flight reported its outcome.
    void complete_tasks(std::ptrdiff_t result) noexcept;

    // Is an operation of this channel's with the kernel? The writes of one
    // operation follow one another in the file, so there is at most one
    // outstanding.
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

    void prepare(std::span<const char> buffer, Foundation::Core::WriteResult &result, Foundation::Async::Coroutine waiter);

    void refresh_arming() noexcept;

    // Retires the finished writes at the front of the queue, in the order the file
    // took them, and hands their waiters back to the scheduler.
    void retire_answered() noexcept;

    // Leaves a waiting frame with an outcome, so that nothing is ever parked for a
    // completion that cannot come. Used when the channel goes away.
    void drop(PendingWrite &pending) noexcept;

    FileStream &file_;
    std::deque<PendingWrite> pending_;
    // How many of `pending_`, from the front, the kernel has been given.
    std::size_t submitted_{0};
    std::vector<::iovec> vectors_;
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
