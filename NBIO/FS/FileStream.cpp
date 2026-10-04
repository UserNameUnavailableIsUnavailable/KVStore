#include "FileStream.hpp"

#include <NBIO/Runtime/Runtime.hpp>
#include <filesystem>

#include "FileReadChannel.hpp"
#include "FileWriteChannel.hpp"

namespace NBIO::FS {
FileStream::FileStream(const std::string& path, NBIO::FS::FileMode mode, std::filesystem::perms permissions,
                       NBIO::Core::Multiplexer& multiplexer, NBIO::Async::Scheduler& scheduler)
    : file_(path, mode, permissions),
      read_channel_(std::make_unique<FileReadChannel>(*this, multiplexer, scheduler)),
      write_channel_(std::make_unique<FileWriteChannel>(*this, multiplexer, scheduler)) {
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (!error && (mode & NBIO::FS::FileMode::kWrite) == NBIO::FS::FileMode::kWrite) {
        write_offset_ = static_cast<std::uint64_t>(size);
    }
}

NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> FileStream::read(std::span<char> buffer) {
    co_return co_await read_channel().read(buffer);
}

NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> FileStream::write(std::span<const char> buffer) {
    co_return co_await write_channel().write(buffer);
}

FileReadChannel& FileStream::read_channel() noexcept { return *read_channel_; }

FileWriteChannel& FileStream::write_channel() noexcept { return *write_channel_; }
}  // namespace NBIO::FS




