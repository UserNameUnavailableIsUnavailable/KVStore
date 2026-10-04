#pragma once

#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Utility/Buffer.hpp>
#include <NBIO/FS/File.hpp>
#include <NBIO/Core/Channel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <NBIO/FS/Payload.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <deque>
#include <optional>
#include <span>
#include <system_error>

namespace NBIO::FS {
class FileStream;

class WriteAwaiter;

class FileWriteChannel final : public NBIO::Core::Channel<FileWriteChannel> {
   public:
    using Payload = detail::IOVectorPayload<FileWriteChannel>;

    FileWriteChannel(FileStream& file, NBIO::Core::Multiplexer& multiplexer,
                     NBIO::Async::Scheduler& scheduler);
    ~FileWriteChannel() noexcept;

    NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> write(std::span<const char> buffer);

    // The operation the backend performs lives in the payload; the backend fills
    // the transmissions and asks the channel to reap them.
    Payload& submit();
    void complete();

    FileStream& file() noexcept { return file_; }
    const FileStream& file() const noexcept { return file_; }

   private:
    friend class WriteAwaiter;

    // Queues the write and arms the channel: this is the suspension point, and
    // being armed is what tells the backend to look at the channel.
    void prepare(Async::Coroutine waiter, FS::Transmission* transmission);

    FileStream& file_;
    // One waiter per transmission, in queue order.
    std::deque<Async::Coroutine> waiters_;
    Payload payload_{};
};
}  // namespace NBIO::FS




