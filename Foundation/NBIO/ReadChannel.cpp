#include "ReadChannel.hpp"
#include <Foundation/Core/File.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "FileStream.hpp"

#include <cerrno>
#include <optional>
#include <system_error>
#include <utility>

#include <unistd.h>

namespace Foundation::NBIO
{
namespace
{
struct ReadAwaiter
{
    ReadChannel &channel;
    std::span<char> buffer;

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        auto coroutine = Async::Coroutine::from_handle(handle);
        channel.arm();
        channel.park(std::move(coroutine));
        channel.job() = {.buffer = buffer,
                            .offset = channel.file_stream().read_offset(),
                            .result = {.status = Foundation::Core::ReadStatus::kPending, .bytes_transferred = 0, .error_code = {}}};
        return true;
    }

    Foundation::Core::ReadResult await_resume() const noexcept
    {
        return channel.job().result;
    }
};
} // namespace

ReadChannel::ReadChannel(FileStream &file, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kRead, file.native_handle(), multiplexer, scheduler), file_(file)
{
    multiplexer_.add_channel(this);
}

ReadChannel::~ReadChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

std::optional<std::size_t> ReadChannel::read_sync(std::span<char> buffer)
{
    while (true)
    {
        const auto chunk = buffer;
        const auto result = ::pread(native_handle(), chunk.data(), chunk.size(), static_cast<off_t>(job_.offset));
        if (result < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            error_code_ = {errno, std::system_category()};
        }
        if (result == 0)
        {
            return 0;
        }

        job_.offset += static_cast<std::uint64_t>(result);
        file_.advance_read_offset(static_cast<std::size_t>(result));
        return result;
    }
}

Foundation::NBIO::Task<std::optional<std::size_t>> ReadChannel::read(std::span<char> buffer)
{
    std::optional<std::size_t> ret{};
    auto result = co_await ReadAwaiter{*this, buffer};
    if (result.status == Core::ReadStatus::kError)
    {
        error_code_ = result.error_code;
    }
    else
    {
        ret = result.bytes_transferred;
    }
    co_return ret;
}

void ReadChannel::handle_event()
{
    if (handler_) [[likely]]
    {
        handler_(this);
    }

    if (!waiter_) [[unlikely]]
    {
        waiter_ = {};
        return;
    }

    if (job_.result.status == Foundation::Core::ReadStatus::kPending) [[unlikely]]
    {
        arm();
        return;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}
} // namespace Foundation::NBIO
