#include "FileStream.hpp"

#include "ReadChannel.hpp"
#include "WriteChannel.hpp"

#include <filesystem>

namespace Foundation::Async
{
FileStream::FileStream(const std::string &path, Foundation::Core::FileMode mode, ::mode_t permissions,
                       Multiplexer &multiplexer, Scheduler &scheduler)
    : file_(path, mode), read_channel_(std::make_unique<ReadChannel>(*this, multiplexer, scheduler)),
      write_channel_(std::make_unique<WriteChannel>(*this, multiplexer, scheduler))
{
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (!error && (mode & Foundation::Core::FileMode::kWrite) == Foundation::Core::FileMode::kWrite)
    {
        write_offset_ = static_cast<std::uint64_t>(size);
    }
    (void)permissions;
}

FileStream::~FileStream() noexcept
{
    close();
}

std::shared_ptr<FileStream> FileStream::Open(const std::string &path, Foundation::Core::FileMode mode, ::mode_t permissions, Multiplexer &multiplexer, Scheduler &scheduler)
{
    return std::make_shared<FileStream>(path, mode, permissions, multiplexer, scheduler);
}

void FileStream::close() noexcept
{
    file_.close();
}

Task<ReadResult> FileStream::read(Foundation::Core::Buffer &buffer)
{
    co_return co_await read_channel().read(buffer);
}

Task<WriteResult> FileStream::write(Foundation::Core::Buffer &buffer)
{
    co_return co_await write_channel().write(buffer);
}

ReadChannel &FileStream::read_channel() noexcept
{
    return *read_channel_;
}

WriteChannel &FileStream::write_channel() noexcept
{
    return *write_channel_;
}
} // namespace Foundation::Async
