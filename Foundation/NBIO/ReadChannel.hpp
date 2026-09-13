#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/File.hpp>
#include <coroutine>
#include <cstdint>


namespace Foundation::NBIO
{
class FileStream;

struct ReadJob
{
    Foundation::Core::Buffer *buffer{nullptr};
    std::uint64_t offset{0};
    Foundation::Core::ReadResult result{};
};

class ReadChannel final : public Foundation::NBIO::Channel
{
  public:
    ReadChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~ReadChannel() noexcept override;

    Foundation::NBIO::Task<Foundation::Core::ReadResult> read(Foundation::Core::Buffer &buffer);

    void handle_event() override;

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    ReadJob &job() noexcept
    {
        return job_;
    }
    const ReadJob &job() const noexcept
    {
        return job_;
    }

    FileStream &file_stream() noexcept
    {
        return file_;
    }
    const FileStream &file_stream() const noexcept
    {
        return file_;
    }

  private:
    Foundation::Core::ReadResult read_sync(Foundation::Core::Buffer &buffer);

    FileStream &file_;
    ReadJob job_;
    Foundation::Async::Coroutine waiter_;
};
} // namespace Foundation::NBIO
