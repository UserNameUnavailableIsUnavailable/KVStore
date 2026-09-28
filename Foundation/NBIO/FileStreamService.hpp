#pragma once

#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Expected.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/FileStream.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <cstddef>
#include <filesystem>
#include <span>
#include <system_error>

namespace Foundation::NBIO
{
// A file, with the two channels that read and write it.
//
// Unlike the runtime's timer and signal, a file is a resource: opening one is what
// makes these channels exist, so this service owns them and is made rather than
// borrowed. It is attached to the engine installed on this thread, because that is
// where the file's readiness is watched -- and it owns the file by value, because one
// owner is the whole point: there is nothing to hand back.
class FileStreamService final
{
  public:
    // Opens `path`, read-write and created if it is not there unless told otherwise.
    // Throws when there is no engine on this thread, or when the file cannot be
    // opened: a service that exists is one that has the file.
    explicit FileStreamService(const std::filesystem::path &path,
                               Foundation::Core::FileMode mode = Foundation::Core::FileMode::kReadWrite |
                                                                Foundation::Core::FileMode::kCreate,
                               std::filesystem::perms permissions = Foundation::Core::File::kDefaultPermissions);

    FileStreamService(const FileStreamService &) = delete;
    FileStreamService &operator=(const FileStreamService &) = delete;
    // Not movable either: the file it owns is not, since the multiplexer holds a
    // pointer to each channel built on it.
    FileStreamService(FileStreamService &&) = delete;
    FileStreamService &operator=(FileStreamService &&) = delete;

    ~FileStreamService() noexcept = default;

    // One read or write at the file's current position, answered with what it moved.
    Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> read(std::span<char> buffer);
    Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> write(std::span<const char> buffer);

    Foundation::NBIO::FileStream &stream() noexcept
    {
        return stream_;
    }

  private:
    FileStream stream_;
};
} // namespace Foundation::NBIO
