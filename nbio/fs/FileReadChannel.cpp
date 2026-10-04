#include "FileReadChannel.hpp"

#include <nbio/fs/File.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <optional>
#include <span>
#include <utility>

#include "FileStream.hpp"

namespace nbio::fs {
class ReadAwaiter {
   public:
    ReadAwaiter(FileReadChannel& channel, std::span<char> buffer) : channel_(channel), buffer_(buffer) {}

    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle) {
        transmission_.status = OperationStatus::kPending;
        transmission_.bytes = 0;
        transmission_.error_code = {};
        transmission_.buffer = buffer_;
        channel_.Prepare(async::Coroutine::FromHandle(handle), &transmission_);
        channel_.Arm();
        return true;
    }

    fs::Transmission await_resume() const noexcept { return transmission_; }

   private:
    FileReadChannel& channel_;
    std::span<char> buffer_;
    fs::Transmission transmission_{};
};

FileReadChannel::FileReadChannel(FileStream& file, nbio::core::Multiplexer& multiplexer,
                                 nbio::async::Scheduler& scheduler)
    : nbio::core::Channel<FileReadChannel>(nbio::core::ChannelType::kRead,
                                                 static_cast<std::uintptr_t>(file.native_handle()), multiplexer,
                                                 scheduler),
      file_stream_(file) {
    // Registered on the first arm(): a file is always ready, so the channel is
    // only watched while a read is queued.
}

FileReadChannel::~FileReadChannel() noexcept { multiplexer_.DeleteChannel(this); }

void FileReadChannel::Prepare(async::Coroutine waiter, fs::Transmission* transmission) {
    waiters_.push_back(std::move(waiter));
    auto& payload = payload_;
    payload.Submit(transmission);
}

FileReadChannel::Payload& FileReadChannel::Submit() {
    auto& payload = payload_;
    payload.set_offset(file_stream_.read_offset());
    return payload_;
}

void FileReadChannel::Complete() {
    auto& payload = payload_;
    // The backend advanced the payload offset by what it read; move the file's
    // cursor along with it.
    file_stream_.AdvanceReadOffset(payload.offset() - file_stream_.read_offset());
    while (auto completion = payload.NextCompletion()) {
        auto waiter = std::move(waiters_.front());
        waiters_.pop_front();
        scheduler_.Submit(std::move(waiter));
        payload.Conclude();
    }
    if (payload.size() != 0) {
        Arm();
    } else {
        Disarm();
    }
}

nbio::async::Task<nbio::runtime, utility::expected<std::size_t, std::error_code>> FileReadChannel::read(std::span<char> buffer) {
    auto result = co_await ReadAwaiter{*this, buffer};
    if (result.status == OperationStatus::kError) {
        co_return utility::unexpected<std::error_code>(std::move(result.error_code));
    }
    co_return result.bytes;
}
}  // namespace nbio::fs




