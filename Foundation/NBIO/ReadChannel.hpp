#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/File.hpp>
#include <cstdint>
#include <optional>
#include <system_error>


namespace Foundation::NBIO
{
class FileStream;

struct ReadJob
{
    std::span<char> buffer;
    std::uint64_t offset{0};
    Foundation::Core::ReadResult result{};
};

class ReadChannel final : public Foundation::NBIO::Channel
{
  public:
    ReadChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~ReadChannel() noexcept override;

    Foundation::NBIO::Task<std::optional<std::size_t>> read(std::span<char> buffer);

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
    std::optional<std::size_t> read_sync(std::span<char> buffer);
    FileStream &file_;
    ReadJob job_;
    Foundation::Async::Coroutine waiter_;
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
