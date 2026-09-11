#pragma once

#include <Foundation/Buffer.hpp>
#include <Foundation/File.hpp>
#include <coroutine>
#include <cstdint>

#include "Channel.hpp"
#include "FileTypes.hpp"
#include "Task.hpp"

namespace Foundation::Async
{
class FileStream;

struct ReadJob
{
    Buffer *buffer{nullptr};
    std::uint64_t offset{0};
    ReadResult result{};
};

class ReadChannel final : public Channel
{
  public:
    ReadChannel(FileStream &file, Multiplexer &multiplexer, Scheduler &scheduler);
    ~ReadChannel() noexcept override;

    Task<ReadResult> read(Buffer &buffer);

    void on_event() override;

    void set_waiter(std::coroutine_handle<> co) noexcept
    {
        waiter_ = co;
    }
    std::coroutine_handle<> get_waiter() const noexcept
    {
        return waiter_;
    }

    ReadJob &job() noexcept
    {
        return job_;
    }
    const ReadJob &job() const noexcept
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
    ReadResult read_sync(Buffer &buffer);

    FileStream &file_;
    ReadJob job_;
    std::coroutine_handle<> waiter_;
};
} // namespace Foundation::Async
