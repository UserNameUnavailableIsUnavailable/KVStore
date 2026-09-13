#include "WriteChannel.hpp"
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "FileStream.hpp"

#include <cerrno>
#include <system_error>
#include <utility>

#include <unistd.h>

namespace Foundation::NBIO
{
namespace
{
struct WriteAwaiter
{
    WriteChannel &channel;
    Foundation::Core::Buffer &buffer;

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        auto coroutine = Async::Coroutine::from_handle(handle);
        channel.park(std::move(coroutine));
        channel.job() = {.buffer = &buffer,
                            .offset = channel.file().write_offset(),
                            .result = {.status = Foundation::Core::WriteStatus::kPending, .bytes_transferred = 0, .error_code = {}}};
        return true;
    }

    Foundation::Core::WriteResult await_resume() const noexcept
    {
        return channel.job().result;
    }
};

Foundation::Core::WriteResult make_write_error(std::size_t transferred)
{
    return {.status = Foundation::Core::WriteStatus::kError,
            .bytes_transferred = transferred,
            .error_code = std::error_code(errno, std::system_category())};
}
} // namespace

WriteChannel::WriteChannel(FileStream &file_stream, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kWrite, file_stream.native_handle(), multiplexer, scheduler), file_(file_stream)
{
    multiplexer_.add_channel(this);
}

WriteChannel::~WriteChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

Foundation::Core::WriteResult WriteChannel::write_sync(Foundation::Core::Buffer &buffer)
{
    auto total = std::size_t{0};
    while (buffer.valid_size() > 0)
    {
        const auto chunk = buffer.valid_span();
        const auto result = ::pwrite(native_handle(), chunk.data(), chunk.size(), static_cast<off_t>(job_.offset));
        if (result < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return make_write_error(total);
        }
        if (result == 0)
        {
                return {.status = Foundation::Core::WriteStatus::kError,
                    .bytes_transferred = total,
                    .error_code = std::make_error_code(std::errc::io_error)};
        }

        buffer.consume(static_cast<std::size_t>(result));
        job_.offset += static_cast<std::uint64_t>(result);
        file_.advance_write_offset(static_cast<std::size_t>(result));
        total += static_cast<std::size_t>(result);
    }

    return {.status = Foundation::Core::WriteStatus::kDone, .bytes_transferred = total, .error_code = {}};
}

Foundation::NBIO::Task<Foundation::Core::WriteResult> WriteChannel::write(Foundation::Core::Buffer &buffer)
{
    co_return co_await WriteAwaiter{*this, buffer};
}

void WriteChannel::handle_event()
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

    if (job_.result.status == Foundation::Core::WriteStatus::kPending)
    {
        arm();
        return;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}
} // namespace Foundation::NBIO
