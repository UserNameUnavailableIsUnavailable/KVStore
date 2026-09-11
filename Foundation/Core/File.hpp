#pragma once

#include <cstdint>
#include <filesystem>
#include <span>
#include <system_error>

#include "Native.hpp"

namespace Foundation::Core
{
enum class FileMode : std::uint32_t
{
    kRead = 1u << 0,
    kWrite = 1u << 1,
    kReadWrite = kRead | kWrite,
    kCreate = 1u << 2,
    kTruncate = 1u << 3,
    kAppend = 1u << 4,
};

enum class ReadStatus
{
    kDone,
    kPending,
    kEndOfFile,
    kError,
};

struct ReadResult
{
    ReadStatus status{ReadStatus::kPending};
    std::size_t bytes_transferred{0};
    std::error_code error_code{};
};

enum class WriteStatus
{
    kDone,
    kPending,
    kError,
};

struct WriteResult
{
    WriteStatus status{WriteStatus::kPending};
    std::size_t bytes_transferred{0};
    std::error_code error_code{};
};

constexpr FileMode operator|(FileMode lhs, FileMode rhs) noexcept
{
    return static_cast<FileMode>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
}

constexpr FileMode operator&(FileMode lhs, FileMode rhs) noexcept
{
    return static_cast<FileMode>(static_cast<std::uint32_t>(lhs) & static_cast<std::uint32_t>(rhs));
}

constexpr FileMode &operator|=(FileMode &lhs, FileMode rhs) noexcept
{
    lhs = lhs | rhs;
    return lhs;
}

class File
{
  public:
    using Handle = NativeHandle;
    explicit File(const std::filesystem::path &path, FileMode mode);
    File(const File &) = delete;
    File &operator=(const File &) = delete;
    File(File &&) = default;
    File &operator=(File &&) = default;
    ~File() noexcept;

    ReadResult read(std::span<char> buffer);
    WriteResult write(std::span<const char> buffer);

    bool is_valid() const noexcept
    {
        return handle_ != kInvalidHandle;
    }

    Handle native_handle() const noexcept
    {
        return handle_;
    }

    FileMode mode() const noexcept
    {
        return mode_;
    }

    int native_flags_for() const noexcept
    {
        return native_flags_for(mode_);
    }

    void close() noexcept;

    static int native_flags_for(FileMode mode);

  private:
    Handle open_file(const std::filesystem::path &path, FileMode mode);

#if defined(_WIN32)
    static constexpr Handle kInvalidHandle{ nullptr };
#else
    static constexpr Handle kInvalidHandle{ -1 };
#endif

    Handle handle_;
    FileMode mode_;
};
} // namespace Foundation::Core