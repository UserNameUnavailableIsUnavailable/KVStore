#pragma once

#include <cstdint>
#include <filesystem>
#include <span>
#include <system_error>

#include "Expected.hpp"

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
    explicit File(const std::filesystem::path &path, FileMode mode);
    File(const File &) = delete;
    File &operator=(const File &) = delete;
    File(File &&) = default;
    File &operator=(File &&) = default;
    ~File() noexcept;

    expected<std::size_t, std::error_code> read(std::span<char> buffer);
    expected<std::size_t, std::error_code> write(std::span<const char> buffer);

    std::uintptr_t native_handle() const noexcept
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

    static int native_flags_for(FileMode mode);

  private:
    std::uintptr_t open_file(const std::filesystem::path &path, FileMode mode);
    void close() noexcept;

    std::uintptr_t handle_;
    FileMode mode_;
};
} // namespace Foundation::Core