#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/File.hpp>
#include <cstdint>
#include <system_error>


namespace Foundation::NBIO
{
class FileStream;

struct WriteJob
{
    std::span<const char> buffer;
    std::uint64_t offset{0};
    Foundation::Core::WriteResult result{};
};

class WriteChannel final : public Foundation::NBIO::Channel
{
  public:
    WriteChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~WriteChannel() noexcept override;

    Foundation::NBIO::Task<std::optional<std::size_t>> write(std::span<const char> buffer);

    void handle_event() override;

    void park(Async::Coroutine coroutine) noexcept
    {
        waiter_ = coroutine;
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
    
    std::error_code last_error() const noexcept
    {
        return error_code_;
    }

  private:
    Foundation::Core::WriteResult write_sync(std::span<const char> buffer);

    FileStream &file_;
    WriteJob job_;
    Async::Coroutine waiter_;
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
