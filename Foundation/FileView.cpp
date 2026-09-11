#include "FileView.hpp"

#include <filesystem>
#include <stdexcept>

#if defined(_WIN32)
#include <Windows.h>
#else
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace Foundation
{
namespace
{
std::size_t file_size_for(const File &file)
{
#if defined(_WIN32)
    LARGE_INTEGER size{};
    if (!::GetFileSizeEx(file.native_handle(), &size))
    {
        throw std::runtime_error("GetFileSizeEx failed");
    }
    return static_cast<std::size_t>(size.QuadPart);
#else
    struct stat info{};
    if (::fstat(file.native_handle(), &info) != 0)
    {
        throw std::runtime_error("fstat failed");
    }
    return static_cast<std::size_t>(info.st_size);
#endif
}
} // namespace

FileView::FileView(const std::filesystem::path &path) : owned_file_(std::make_unique<File>(path, FileMode::kRead))
{
    file_ = owned_file_.get();
    mode_ = file_->mode();
    size_ = file_size_for(*file_);
    if (size_ == 0)
    {
        data_ = nullptr;
        return;
    }

#if defined(_WIN32)
    mapping_handle_ = ::CreateFileMappingW(file_->native_handle(), nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (!mapping_handle_)
    {
        throw std::runtime_error("CreateFileMappingW failed");
    }
    data_ = static_cast<char *>(::MapViewOfFile(mapping_handle_, FILE_MAP_READ, 0, 0, 0));
    if (!data_)
    {
        ::CloseHandle(mapping_handle_);
        mapping_handle_ = nullptr;
        throw std::runtime_error("MapViewOfFile failed");
    }
#else
    void *mapped = ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, file_->native_handle(), 0);
    if (mapped == MAP_FAILED)
    {
        throw std::runtime_error("mmap failed");
    }
    data_ = static_cast<char *>(mapped);
#endif
}

FileView::FileView(File &file) : file_(&file), mode_(file.mode())
{
    size_ = file_size_for(file);
    if (size_ == 0)
    {
        data_ = nullptr;
        return;
    }

#if defined(_WIN32)
    const auto protection = (mode_ & FileMode::kWrite) == FileMode::kWrite ? PAGE_READWRITE : PAGE_READONLY;
    mapping_handle_ = ::CreateFileMappingW(file_->native_handle(), nullptr, protection, 0, 0, nullptr);
    if (!mapping_handle_)
    {
        throw std::runtime_error("CreateFileMappingW failed");
    }
    const auto desired_access = (mode_ & FileMode::kWrite) == FileMode::kWrite ? (FILE_MAP_READ | FILE_MAP_WRITE) : FILE_MAP_READ;
    data_ = static_cast<char *>(::MapViewOfFile(mapping_handle_, desired_access, 0, 0, 0));
    if (!data_)
    {
        ::CloseHandle(mapping_handle_);
        mapping_handle_ = nullptr;
        throw std::runtime_error("MapViewOfFile failed");
    }
#else
    int protection = PROT_READ;
    int flags = MAP_SHARED;
    if ((mode_ & FileMode::kWrite) == FileMode::kWrite)
    {
        protection |= PROT_WRITE;
    }
    void *mapped = ::mmap(nullptr, size_, protection, flags, file_->native_handle(), 0);
    if (mapped == MAP_FAILED)
    {
        throw std::runtime_error("mmap failed");
    }
    data_ = static_cast<char *>(mapped);
#endif
}

FileView::~FileView() noexcept
{
#if defined(_WIN32)
    if (data_ != nullptr)
    {
        ::UnmapViewOfFile(data_);
    }
    if (mapping_handle_)
    {
        ::CloseHandle(mapping_handle_);
    }
#else
    if (data_ != nullptr)
    {
        ::munmap(data_, size_);
    }
#endif
}
} // namespace Foundation