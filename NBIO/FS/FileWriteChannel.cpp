#include "FileWriteChannel.hpp"

#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/FS/File.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <optional>
#include <span>
#include <utility>

#include "FileStream.hpp"

namespace NBIO::FS {
class WriteAwaiter {
   public:
    WriteAwaiter(FileWriteChannel& channel, std::span<const char> buffer) : channel_(channel), buffer_(buffer) {}

    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle) {
        transmission_.status = OperationStatus::kPending;
        transmission_.bytes = 0;
        transmission_.error_code = {};
        // The transmission buffer is non-const only for C API compatibility: the
        // backend reads it, never writes through it.
        transmission_.buffer = std::span<char>(const_cast<char*>(buffer_.data()), buffer_.size());
        channel_.prepare(Async::Coroutine::from_handle(handle), &transmission_);
        channel_.arm();
        return true;
    }

    FS::Transmission await_resume() const noexcept { return transmission_; }

   private:
    FileWriteChannel& channel_;
    std::span<const char> buffer_;
    FS::Transmission transmission_{};
};

FileWriteChannel::FileWriteChannel(FileStream& file_stream, NBIO::Core::Multiplexer& multiplexer,
                                   NBIO::Async::Scheduler& scheduler)
    : NBIO::Core::Channel<FileWriteChannel>(NBIO::Core::ChannelType::kWrite, file_stream.native_handle(),
                                                  multiplexer, scheduler),
      file_(file_stream) {
    // Registered on the first arm(): a file is always ready, so the channel is
    // only watched while a write is queued.
}

FileWriteChannel::~FileWriteChannel() noexcept { multiplexer_.delete_channel(this); }

void FileWriteChannel::prepare(Async::Coroutine waiter, FS::Transmission* transmission) {
    waiters_.push_back(std::move(waiter));
    auto& payload = payload_;
    payload.submit(transmission);
}

FileWriteChannel::Payload& FileWriteChannel::submit() {
    auto& payload = payload_;
    payload.set_offset(file_.write_offset());
    return payload_;
}

void FileWriteChannel::complete() {
    auto& payload = payload_;
    // The backend advanced the payload offset by what it wrote; move the file's
    // cursor along with it.
    file_.advance_write_offset(payload.offset() - file_.write_offset());
    while (auto completion = payload.next_completion()) {
        auto waiter = std::move(waiters_.front());
        waiters_.pop_front();
        scheduler_.submit(std::move(waiter));
        payload.conclude();
    }
    if (payload.size() != 0) {
        arm();
    } else {
        disarm();
    }
}

NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> FileWriteChannel::write(
    std::span<const char> buffer) {
    auto result = co_await WriteAwaiter{*this, buffer};
    if (result.status == OperationStatus::kError) {
        co_return Utility::unexpected<std::error_code>(std::move(result.error_code));
    }
    co_return result.bytes;
}
}  // namespace NBIO::FS




