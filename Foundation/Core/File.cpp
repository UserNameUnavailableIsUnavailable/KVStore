#include "File.hpp"

#include <cerrno>
#include <stdexcept>
#include <system_error>

#if defined(_WIN32)
#include <Windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace Foundation::Core
{
File::File(const std::filesystem::path &path, FileMode mode) : handle_(open_file(path, mode)), mode_(mode)
{
}

File::~File() noexcept
{
    close();
}

ReadResult File::read(std::span<char> buffer)
{
#if defined(_WIN32)
    (void)buffer;
    throw std::runtime_error("Windows file read is not implemented yet");
#else
    while (true)
    {
        const auto result = ::read(handle_, buffer.data(), buffer.size());
        if (result < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return {.status = ReadStatus::kError,
                    .bytes_transferred = 0,
                    .error_code = std::error_code(errno, std::system_category())};
        }
        if (result == 0)
        {
            return {.status = ReadStatus::kEndOfFile, .bytes_transferred = 0, .error_code = {}};
        }

        return {.status = ReadStatus::kDone,
                .bytes_transferred = static_cast<std::size_t>(result),
                .error_code = {}};
    }
#endif
}

WriteResult File::write(std::span<const char> buffer)
{
#if defined(_WIN32)
    (void)buffer;
    static_assert(false, "Windows file write is not implemented yet");
#else
    auto total = std::size_t{0};
    while (buffer.size() > 0)
    {
        const auto result = ::write(handle_, buffer.data(), buffer.size());
        if (result < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return {.status = WriteStatus::kError,
                    .bytes_transferred = total,
                    .error_code = std::error_code(errno, std::system_category())};
        }
        if (result == 0)
        {
            return {.status = WriteStatus::kError,
                    .bytes_transferred = total,
                    .error_code = std::make_error_code(std::errc::io_error)};
        }

        total += static_cast<std::size_t>(result);
        buffer = buffer.subspan(static_cast<std::size_t>(result));
    }

    return {.status = WriteStatus::kDone, .bytes_transferred = total, .error_code = {}};
#endif
}

int File::native_flags_for(FileMode mode)
{
#if defined(_WIN32)
    (void)mode;
    return 0;
#else
    const auto access = static_cast<std::uint32_t>(mode) & static_cast<std::uint32_t>(FileMode::kReadWrite);
    int flags = 0;
    switch (static_cast<FileMode>(access))
    {
    case FileMode::kRead:
        flags |= O_RDONLY;
        break;
    case FileMode::kWrite:
        flags |= O_WRONLY;
        break;
    case FileMode::kReadWrite:
        flags |= O_RDWR;
        break;
    default:
        throw std::invalid_argument("invalid file open access mode");
    }

    if ((mode & FileMode::kCreate) == FileMode::kCreate)
    {
        flags |= O_CREAT;
    }
    if ((mode & FileMode::kTruncate) == FileMode::kTruncate)
    {
        flags |= O_TRUNC;
    }
    if ((mode & FileMode::kAppend) == FileMode::kAppend)
    {
        flags |= O_APPEND;
    }
    return flags;
#endif
}

std::uintptr_t File::open_file(const std::filesystem::path &path, FileMode mode)
{
#if defined(_WIN32)
    (void)path;
    (void)mode;
    static_assert(false, "Windows file open is not implemented yet");
#else
    const auto fd = ::open(path.c_str(), native_flags_for(mode), 0644);
    if (fd < 0)
    {
        throw std::system_error(errno, std::system_category(), "open failed");
    }
    return fd;
#endif
}

void File::close() noexcept
{
#if defined(_WIN32)
    ::CloseHandle(handle_);
    handle_ = kInvalidHandle;
#else
    ::close(handle_);
#endif
}
} // namespace Foundation::Core