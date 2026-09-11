#pragma once

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/File.hpp>

#include <cstdint>
#include <memory>
#include <string>

#if not defined(__linux__)
#error "Async::FileStream is only supported on Linux"
#endif

#include <sys/types.h>

#include "FileTypes.hpp"
#include "Multiplexer.hpp"
#include "ReadChannel.hpp"
#include "Scheduler.hpp"
#include "Task.hpp"
#include "WriteChannel.hpp"

namespace Foundation::Async
{
class ReadChannel;
class WriteChannel;

class FileStream : protected std::enable_shared_from_this<FileStream>
{
  public:
    FileStream(const std::string &path, Foundation::Core::FileMode mode, ::mode_t permissions, Multiplexer &multiplexer,
               Scheduler &scheduler);
    FileStream(const FileStream &) = delete;
    FileStream &operator=(const FileStream &) = delete;
    FileStream(FileStream &&) = delete;
    FileStream &operator=(FileStream &&) = delete;
    ~FileStream() noexcept;

    static std::shared_ptr<FileStream> Open(const std::string &path, Foundation::Core::FileMode mode, ::mode_t permissions,
                                            Multiplexer &multiplexer, Scheduler &scheduler);

    Task<ReadResult> read(Foundation::Core::Buffer &buffer);
    Task<WriteResult> write(Foundation::Core::Buffer &buffer);

    void close() noexcept;

    Foundation::Core::File::Handle native_handle() const noexcept
    {
        return file_.native_handle();
    }

    std::uint64_t read_offset() const noexcept
    {
        return read_offset_;
    }
    std::uint64_t write_offset() const noexcept
    {
        return write_offset_;
    }
    void advance_read_offset(std::size_t bytes) noexcept
    {
        read_offset_ += static_cast<std::uint64_t>(bytes);
    }
    void advance_write_offset(std::size_t bytes) noexcept
    {
        write_offset_ += static_cast<std::uint64_t>(bytes);
    }

    ReadChannel &read_channel() noexcept;
    WriteChannel &write_channel() noexcept;

  private:
    Foundation::Core::File file_;
    std::unique_ptr<ReadChannel> read_channel_;
    std::unique_ptr<WriteChannel> write_channel_;
    std::uint64_t read_offset_{0};
    std::uint64_t write_offset_{0};
};

using FileStreamPtr = std::shared_ptr<FileStream>;
} // namespace Foundation::Async
