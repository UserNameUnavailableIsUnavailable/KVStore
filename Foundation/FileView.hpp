#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>

#include "File.hpp"
#include "Native.hpp"

namespace Foundation
{

class FileView
{
  public:
    using Handle = NativeHandle;
    explicit FileView(const std::filesystem::path &path);
    explicit FileView(File &file);
    FileView(const FileView &) = delete;
    FileView &operator=(const FileView &) = delete;
    FileView(FileView &&) = delete;
    FileView &operator=(FileView &&) = delete;
    ~FileView() noexcept;

    std::size_t size() const noexcept
    {
        return size_;
    }

    const void *data() const noexcept
    {
        return data_;
    }

    void *data() noexcept
    {
        return data_;
    }

    bool is_valid() const noexcept
    {
        return data_ != nullptr || size_ == 0;
    }

  private:
#if defined(_WIN32)
    using MappingHandle = void *;
#endif

    std::unique_ptr<File> owned_file_;
    File *file_{nullptr};
#if defined(_WIN32)
    MappingHandle mapping_handle_{nullptr};
#endif
    void *data_{nullptr};
    std::size_t size_{0};
    FileMode mode_{FileMode::kRead};
};
} // namespace Foundation