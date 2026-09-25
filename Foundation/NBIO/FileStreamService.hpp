#pragma once

#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Expected.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/FileStream.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <cstddef>
#include <filesystem>
#include <memory>
#include <span>
#include <system_error>

namespace Foundation::NBIO
{
// A file, with the two channels that read and write it.
//
// Unlike the runtime's timer and signal, a file is a resource: opening one is what
// makes these channels exist, so this service owns them and is made rather than
// borrowed. It is attached to the engine installed on this thread, because that is
// where the file's readiness is watched.
class FileStreamService final
{
  public:
    // Opens `path`, read-write and created if it is not there unless told otherwise.
    // Throws when there is no engine on this thread, or when the file cannot be
    // opened: a service that exists is one that has the file.
    explicit FileStreamService(const std::filesystem::path &path,
                               Foundation::Core::FileMode mode = Foundation::Core::FileMode::kReadWrite |
                                                                Foundation::Core::FileMode::kCreate,
                               ::mode_t permissions = 0644);

    FileStreamService(const FileStreamService &) = delete;
    FileStreamService &operator=(const FileStreamService &) = delete;
    FileStreamService(FileStreamService &&) noexcept = default;
    FileStreamService &operator=(FileStreamService &&) noexcept = default;

    ~FileStreamService() noexcept = default;

    // One read or write at the file's current position, answered with what it moved.
    Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> read(std::span<char> buffer);
    Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> write(std::span<const char> buffer);

    Foundation::NBIO::FileStream &stream() noexcept
    {
        return *stream_;
    }

  private:
    std::shared_ptr<FileStream> stream_;
};
} // namespace Foundation::NBIO
