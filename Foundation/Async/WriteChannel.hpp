#pragma once

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/File.hpp>
#include <coroutine>
#include <cstdint>

#include "Channel.hpp"
#include "FileTypes.hpp"
#include "Task.hpp"

namespace Foundation::Async
{
class FileStream;

struct WriteJob
{
    Foundation::Core::Buffer *buffer{nullptr};
    std::uint64_t offset{0};
    WriteResult result{};
};

class WriteChannel final : public Channel
{
  public:
    WriteChannel(FileStream &file, Multiplexer &multiplexer, Scheduler &scheduler);
    ~WriteChannel() noexcept override;

    Task<WriteResult> write(Foundation::Core::Buffer &buffer);

    void on_event() override;

    void set_waiter(std::coroutine_handle<> co) noexcept
    {
        waiter_ = co;
    }
    std::coroutine_handle<> get_waiter() const noexcept
    {
        return waiter_;
    }

    WriteJob &job() noexcept
    {
        return job_;
    }
    const WriteJob &job() const noexcept
    {
        return job_;
    }

    FileStream &file() noexcept
    {
        return file_;
    }
    const FileStream &file() const noexcept
    {
        return file_;
    }

  private:
    WriteResult write_sync(Foundation::Core::Buffer &buffer);

    FileStream &file_;
    WriteJob job_;
    std::coroutine_handle<> waiter_;
};
} // namespace Foundation::Async
