#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/File.hpp>
#include <Foundation/NBIO/Payload.hpp>
#include <deque>
#include <optional>
#include <span>
#include <system_error>

namespace Foundation::NBIO
{
class FileStream;

class WriteAwaiter;

class FileWriteChannel final : public Foundation::NBIO::Channel
{
  public:
    FileWriteChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~FileWriteChannel() noexcept;

    Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> write(std::span<const char> buffer);

    // The operation the backend performs lives in the payload; the backend fills
    // the transmissions and asks the channel to reap them.
    Payload &submit();
    void complete();

    FileStream &file() noexcept
    {
        return file_;
    }
    const FileStream &file() const noexcept
    {
        return file_;
    }

  private:
    friend class WriteAwaiter;

    // Queues the write and arms the channel: this is the suspension point, and
    // being armed is what tells the backend to look at the channel.
    void prepare(Async::Coroutine waiter, Core::Transmission *transmission);

    FileStream &file_;
    // One waiter per transmission, in queue order.
    std::deque<Async::Coroutine> waiters_;
    Payload payload_{WritePayload{}};
};
} // namespace Foundation::NBIO
