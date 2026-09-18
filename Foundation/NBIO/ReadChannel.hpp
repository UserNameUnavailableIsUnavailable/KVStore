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

// One read waiting its turn. The buffer and the slot the outcome is written into
// belong to the frame that is parked, so several of them can be handed to the
// kernel at once without the channel owning any of the bytes.
struct PendingRead
{
    std::span<char> buffer;
    std::uint64_t offset{0};
    Foundation::Core::ReadResult *result{nullptr};
    Foundation::Async::Coroutine waiter;
};

class ReadChannel final : public Foundation::NBIO::Channel
{
  public:
    ReadChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~ReadChannel() noexcept override;

    Foundation::NBIO::Task<std::optional<std::size_t>> read(std::span<char> buffer);

    void handle_event() override;

    // Hands a read to the channel: it joins the batch in flight when there is one
    // and starts one otherwise, with `result` left holding the outcome.
    void submit(std::span<char> buffer, Foundation::Core::ReadResult &result, Async::Coroutine waiter);

    // The reads the kernel has been given, in the order they will be filled.
    std::span<const ::iovec> vectors() const noexcept
    {
        return std::span<const ::iovec>{vectors_.data(), armed_};
    }

    std::uint64_t front_offset() const noexcept
    {
        return pending_.empty() ? 0 : pending_.front().offset;
    }

    // What the kernel took: a byte count, or a negative errno. A read that got
    // some of what it asked for is complete -- that is what reading is -- and the
    // reads behind it, which the kernel did not reach, are left for the next
    // batch. Nothing at all means the file ended.
    void complete(std::ptrdiff_t result) noexcept;

    // Does the armed reads here and now, for a multiplexer that drives the file
    // itself rather than waiting for a completion.
    void flush() noexcept;

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
    void arm_batch();
    void refresh_vectors();
    void fail(PendingRead &pending) noexcept;

    FileStream &file_;
    std::deque<PendingRead> pending_;
    std::vector<::iovec> vectors_;
    std::size_t armed_{0};
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
