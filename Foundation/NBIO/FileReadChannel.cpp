#include "FileReadChannel.hpp"
#include <Foundation/Core/File.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "FileStream.hpp"

#include <optional>
#include <span>
#include <utility>

namespace Foundation::NBIO
{
class ReadAwaiter
{
  public:
    ReadAwaiter(FileReadChannel &channel, std::span<char> buffer) : channel_(channel), buffer_(buffer)
    {
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType> bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        transmission_.status = Core::OperationStatus::kPending;
        transmission_.bytes = 0;
        transmission_.error_code = {};
        transmission_.buffer = buffer_;
        channel_.prepare(Async::Coroutine::from_handle(handle), &transmission_);
        channel_.arm();
        return true;
    }

    Core::Transmission await_resume() const noexcept
    {
        return transmission_;
    }

  private:
    FileReadChannel &channel_;
    std::span<char> buffer_;
    Core::Transmission transmission_{};
};

FileReadChannel::FileReadChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRead, static_cast<std::uintptr_t>(file.native_handle()), multiplexer, scheduler),
      file_(file)
{
    // Registered on the first arm(): a file is always ready, so the channel is
    // only watched while a read is queued.
}

FileReadChannel::~FileReadChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void FileReadChannel::prepare(Async::Coroutine waiter, Core::Transmission *transmission)
{
    waiters_.push_back(std::move(waiter));
    auto &payload = std::get<ReadPayload>(payload_);
    payload.submit(transmission);
}

Payload &FileReadChannel::submit()
{
    auto &payload = std::get<ReadPayload>(payload_);
    payload.set_offset(file_.read_offset());
    return payload_;
}

void FileReadChannel::complete()
{
    auto &payload = std::get<ReadPayload>(payload_);
    // The backend advanced the payload offset by what it read; move the file's
    // cursor along with it.
    file_.advance_read_offset(payload.offset() - file_.read_offset());
    while (auto completion = payload.next_completion())
    {
        auto waiter = std::move(waiters_.front());
        waiters_.pop_front();
        scheduler_.submit(std::move(waiter));
        payload.conclude();
    }
    if (payload.size() != 0)
    {
        arm();
    }
    else
    {
        disarm();
    }
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> FileReadChannel::read(std::span<char> buffer)
{
    auto result = co_await ReadAwaiter{*this, buffer};
    if (result.status == Core::OperationStatus::kError)
    {
        co_return Core::unexpected<std::error_code>(std::move(result.error_code));
    }
    co_return result.bytes;
}
} // namespace Foundation::NBIO
