#pragma once

#include <nbio/async/Scheduler.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/utility/Buffer.hpp>
#include <nbio/fs/File.hpp>
#include <nbio/core/Channel.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#if not defined(__linux__)
#error "async::FileStream is only supported on Linux"
#endif

#include "FileReadChannel.hpp"
#include "FileWriteChannel.hpp"

namespace nbio::fs {
class FileReadChannel;
class FileWriteChannel;

// One file, and the two channels that read and write it.
//
// Strictly owned: made in place where it lives, never moved and never shared. It has
// to stay put because the multiplexer holds a pointer to each channel, and because
// each channel holds a reference to this -- and it is shared with nobody, because
// the channels are its only referrers and it is the one thing they refer to.
class FileStream {
   public:
    FileStream(const std::string& path, nbio::fs::FileMode mode, std::filesystem::perms permissions,
               nbio::core::Multiplexer& multiplexer, nbio::async::Scheduler& scheduler);
    FileStream(const FileStream&) = delete;
    FileStream& operator=(const FileStream&) = delete;
    FileStream(FileStream&&) = delete;
    FileStream& operator=(FileStream&&) = delete;
    // The file is closed by `file_`'s own destructor, which runs *after* the
    // channels are destroyed. Closing it here, in the body, would release the
    // descriptor while a channel still names it -- and the number could be handed
    // to another open before that channel unregisters itself.
    ~FileStream() noexcept = default;

    nbio::async::Task<nbio::runtime, utility::expected<std::size_t, std::error_code>> read(std::span<char> buffer);
    nbio::async::Task<nbio::runtime, utility::expected<std::size_t, std::error_code>> write(std::span<const char> buffer);

    std::uintptr_t native_handle() const noexcept { return file_.native_handle(); }

    std::uint64_t read_offset() const noexcept { return read_offset_; }
    std::uint64_t write_offset() const noexcept { return write_offset_; }
    void AdvanceReadOffset(std::size_t bytes) noexcept { read_offset_ += static_cast<std::uint64_t>(bytes); }
    void AdvanceWriteOffset(std::size_t bytes) noexcept { write_offset_ += static_cast<std::uint64_t>(bytes); }

    FileReadChannel& read_channel() noexcept;
    FileWriteChannel& write_channel() noexcept;

   private:
    nbio::fs::File file_;
    std::unique_ptr<FileReadChannel> read_channel_;
    std::unique_ptr<FileWriteChannel> write_channel_;
    std::uint64_t read_offset_{0};
    std::uint64_t write_offset_{0};
};

}  // namespace nbio::fs




