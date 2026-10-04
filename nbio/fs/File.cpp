#include "File.hpp"

#include <cerrno>
#include <stdexcept>
#include <system_error>

#if defined(_WIN32)
#include <Windows.h>
#else
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace nbio::fs {
File::File(const std::filesystem::path& path, FileMode mode, std::filesystem::perms permissions)
    : handle_(OpenFile(path, mode, permissions)), mode_(mode) {}

File::~File() noexcept { Close(); }

utility::expected<std::size_t, std::error_code> File::Read(std::span<char> buffer) {
#if defined(_WIN32)
    (void)buffer;
    return utility::unexpected<std::error_code>(std::make_error_code(std::errc::function_not_supported));
#else
    while (true) {
        const auto result = ::read(handle_, buffer.data(), buffer.size());
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return utility::unexpected<std::error_code>(std::error_code(errno, std::system_category()));
        }
        // result == 0 is end-of-file: a successful read of nothing.
        return static_cast<std::size_t>(result);
    }
#endif
}

utility::expected<std::size_t, std::error_code> File::Write(std::span<const char> buffer) {
#if defined(_WIN32)
    (void)buffer;
    return utility::unexpected<std::error_code>(std::make_error_code(std::errc::function_not_supported));
#else
    auto total = std::size_t{0};
    while (!buffer.empty()) {
        const auto result = ::write(handle_, buffer.data(), buffer.size());
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return utility::unexpected<std::error_code>(std::error_code(errno, std::system_category()));
        }
        if (result == 0) {
            return utility::unexpected<std::error_code>(std::make_error_code(std::errc::io_error));
        }

        total += static_cast<std::size_t>(result);
        buffer = buffer.subspan(static_cast<std::size_t>(result));
    }

    return total;
#endif
}

int File::NativeFlagsFor(FileMode mode) {
#if defined(_WIN32)
    (void)mode;
    return 0;
#else
    const auto access = static_cast<std::uint32_t>(mode) & static_cast<std::uint32_t>(FileMode::kReadWrite);
    int flags = 0;
    switch (static_cast<FileMode>(access)) {
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

    if ((mode & FileMode::kCreate) == FileMode::kCreate) {
        flags |= O_CREAT;
    }
    if ((mode & FileMode::kTruncate) == FileMode::kTruncate) {
        flags |= O_TRUNC;
    }
    if ((mode & FileMode::kAppend) == FileMode::kAppend) {
        flags |= O_APPEND;
    }
    return flags;
#endif
}

std::uintptr_t File::OpenFile(const std::filesystem::path& path, FileMode mode, std::filesystem::perms permissions) {
#if defined(_WIN32)
    (void)path;
    (void)mode;
    (void)permissions;
    static_assert(false, "Windows file open is not implemented yet");
#else
    // The only place the native mode is spelled out: everywhere above this line the
    // permissions are std::filesystem::perms, which is what the standard library has
    // and what a caller can name without knowing the platform.
    const auto native = static_cast< ::mode_t>(permissions & std::filesystem::perms::mask);
    const auto fd = ::open(path.c_str(), NativeFlagsFor(mode), native);
    if (fd < 0) {
        throw std::system_error(errno, std::system_category(), "open failed");
    }
    return fd;
#endif
}

void File::Close() noexcept {
#if defined(_WIN32)
    ::CloseHandle(handle_);
    handle_ = kInvalidHandle;
#else
    ::close(handle_);
#endif
}
}  // namespace nbio::fs

