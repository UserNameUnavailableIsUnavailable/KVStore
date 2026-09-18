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

// One write waiting for its turn on the channel. The bytes and the slot the
// outcome is written into both belong to the frame that is parked, which is alive
// for exactly as long as the entry is queued -- and the channel writes into that
// slot rather than into a job of its own, because several of these are handed to
// the kernel at once.
struct PendingWrite
{
    std::span<const char> buffer;
    // Where the rest of this write goes in the file. Set when the write joins an
    // operation and advanced by whatever a short write leaves behind.
    std::uint64_t offset{0};
    Foundation::Core::WriteResult *result{nullptr};
    Foundation::Async::Coroutine waiter;
};

class WriteChannel final : public Foundation::NBIO::Channel
{
  public:
    WriteChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~WriteChannel() noexcept override;

    Foundation::NBIO::Task<std::optional<std::size_t>> write(std::span<const char> buffer);

    void handle_event() override;

    // Hands a write to the channel: it joins the operation in flight when there is
    // one, and starts one otherwise. A file shared by many coroutines -- the
    // append-only file every client session writes to -- therefore writes its
    // entries one at a time, in the order they arrived, each at its own offset.
    //
    // `result` belongs to the waiting frame and is where the outcome is left.
    void submit(std::span<const char> buffer, Foundation::Core::WriteResult &result, Async::Coroutine waiter);

    // The writes the kernel has been given, in the order they will land: one entry
    // per write, so the multiplexer can hand the whole lot over in one operation
    // -- pwritev, or one io_uring submission -- instead of one per write.
    std::span<const ::iovec> vectors() const noexcept
    {
        return std::span<const ::iovec>{vectors_.data(), armed_};
    }

    // Where the first of them lands.
    std::uint64_t front_offset() const noexcept
    {
        return pending_.empty() ? 0 : pending_.front().offset;
    }

    // What the kernel took: `result` is a byte count, or a negative errno. The
    // writes it covers are marked done, the one it stopped halfway through keeps
    // the rest of its bytes, and the ones behind it are left alone.
    void complete(std::ptrdiff_t result) noexcept;

    // Does the armed writes here and now, for a multiplexer that has to drive the
    // file itself rather than wait for a completion.
    void flush() noexcept;

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
    // Takes the writes that are not in an operation yet, gives them consecutive
    // offsets after everything already armed, and arms them as one batch.
    void arm_batch();

    // Rebuilds the vector array from the writes still armed, after a short write
    // moved the front one along.
    void refresh_vectors();

    // Leaves a waiting frame with an outcome, so that nothing is ever parked for a
    // completion that cannot come. Used when the channel goes away.
    void fail(PendingWrite &pending) noexcept;

    FileStream &file_;
    std::deque<PendingWrite> pending_;
    std::vector<::iovec> vectors_;
    std::size_t armed_{0};
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
