#include "FileStream.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include "FileReadChannel.hpp"
#include "FileWriteChannel.hpp"

#include <filesystem>

namespace Foundation::NBIO
{
FileStream::FileStream(const std::string &path, Foundation::Core::FileMode mode, ::mode_t permissions,
                       Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : file_(path, mode), read_channel_(std::make_unique<FileReadChannel>(*this, multiplexer, scheduler)),
      write_channel_(std::make_unique<FileWriteChannel>(*this, multiplexer, scheduler))
{
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (!error && (mode & Foundation::Core::FileMode::kWrite) == Foundation::Core::FileMode::kWrite)
    {
        write_offset_ = static_cast<std::uint64_t>(size);
    }
    (void)permissions;
}

std::shared_ptr<FileStream> FileStream::Open(const std::string &path, Foundation::Core::FileMode mode, ::mode_t permissions, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
{
    return std::make_shared<FileStream>(path, mode, permissions, multiplexer, scheduler);
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> FileStream::read(std::span<char> buffer)
{
    co_return co_await read_channel().read(buffer);
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> FileStream::write(std::span<const char> buffer)
{
    co_return co_await write_channel().write(buffer);
}

FileReadChannel &FileStream::read_channel() noexcept
{
    return *read_channel_;
}

FileWriteChannel &FileStream::write_channel() noexcept
{
    return *write_channel_;
}
} // namespace Foundation::NBIO
