#include "FileStream.hpp"

#include <nbio/runtime/Runtime.hpp>
#include <filesystem>

#include "FileReadChannel.hpp"
#include "FileWriteChannel.hpp"

namespace nbio::fs {
FileStream::FileStream(const std::string& path, nbio::fs::FileMode mode, std::filesystem::perms permissions,
                       nbio::core::Multiplexer& multiplexer, nbio::async::Scheduler& scheduler)
    : file_(path, mode, permissions),
      read_channel_(std::make_unique<FileReadChannel>(*this, multiplexer, scheduler)),
      write_channel_(std::make_unique<FileWriteChannel>(*this, multiplexer, scheduler)) {
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (!error && (mode & nbio::fs::FileMode::kWrite) == nbio::fs::FileMode::kWrite) {
        write_offset_ = static_cast<std::uint64_t>(size);
    }
}

nbio::async::Task<nbio::runtime, utility::expected<std::size_t, std::error_code>> FileStream::read(std::span<char> buffer) {
    co_return co_await read_channel().read(buffer);
}

nbio::async::Task<nbio::runtime, utility::expected<std::size_t, std::error_code>> FileStream::write(std::span<const char> buffer) {
    co_return co_await write_channel().write(buffer);
}

FileReadChannel& FileStream::read_channel() noexcept { return *read_channel_; }

FileWriteChannel& FileStream::write_channel() noexcept { return *write_channel_; }
}  // namespace nbio::fs




