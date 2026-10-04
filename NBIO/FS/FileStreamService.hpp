#pragma once

#include <NBIO/Async/Task.hpp>
#include <NBIO/Utility/Expected.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/FS/FileStream.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <cstddef>
#include <filesystem>
#include <span>
#include <system_error>

namespace NBIO::FS {
// A file, with the two channels that read and write it.
//
// Unlike the runtime's timer and signal, a file is a resource: opening one is what
// makes these channels exist, so this service owns them and is made rather than
// borrowed. It is attached to the engine installed on this thread, because that is
// where the file's readiness is watched -- and it owns the file by value, because one
// owner is the whole point: there is nothing to hand back.
class FileStreamService final {
   public:
    // Opens `path`, read-write and created if it is not there unless told otherwise.
    // Throws when there is no engine on this thread, or when the file cannot be
    // opened: a service that exists is one that has the file.
    explicit FileStreamService(const std::filesystem::path& path,
                               NBIO::FS::FileMode mode = NBIO::FS::FileMode::kReadWrite |
                                                                 NBIO::FS::FileMode::kCreate,
                               std::filesystem::perms permissions = NBIO::FS::File::kDefaultPermissions);

    FileStreamService(const FileStreamService&) = delete;
    FileStreamService& operator=(const FileStreamService&) = delete;
    // Not movable either: the file it owns is not, since the multiplexer holds a
    // pointer to each channel built on it.
    FileStreamService(FileStreamService&&) = delete;
    FileStreamService& operator=(FileStreamService&&) = delete;

    ~FileStreamService() noexcept = default;

    // One read or write at the file's current position, answered with what it moved.
    NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> read(std::span<char> buffer);
    NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> write(std::span<const char> buffer);

    NBIO::FS::FileStream& stream() noexcept { return stream_; }

   private:
    FileStream stream_;
};
}  // namespace NBIO::FS




