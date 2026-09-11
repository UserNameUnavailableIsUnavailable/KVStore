#include "WriteChannel.hpp"

#include "FileStream.hpp"

#include <cerrno>
#include <system_error>
#include <utility>

#include <unistd.h>

namespace Foundation::Async
{
namespace
{
WriteResult make_write_error(std::size_t transferred)
{
    return {.status = WriteStatus::kError,
            .bytes_transferred = transferred,
            .error_code = std::error_code(errno, std::system_category())};
}
} // namespace

WriteChannel::WriteChannel(FileStream &file_stream, Multiplexer &multiplexer, Scheduler &scheduler)
    : Channel(ChannelType::kWrite, file_stream.native_handle(), multiplexer, scheduler), file_(file_stream)
{
    multiplexer_.add_channel(this);
}

WriteChannel::~WriteChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

WriteResult WriteChannel::write_sync(Buffer &buffer)
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
                return {.status = WriteStatus::kError,
                    .bytes_transferred = total,
                    .error_code = std::make_error_code(std::errc::io_error)};
        }

        buffer.consume(static_cast<std::size_t>(result));
        job_.offset += static_cast<std::uint64_t>(result);
        file_.advance_write_offset(static_cast<std::size_t>(result));
        total += static_cast<std::size_t>(result);
    }

    return {.status = WriteStatus::kDone, .bytes_transferred = total, .error_code = {}};
}

Task<WriteResult> WriteChannel::write(Buffer &buffer)
{
    struct Awaiter
    {
        WriteChannel *channel;
        Buffer &buffer;

        bool await_ready() const noexcept
        {
            return false;
        }

        bool await_suspend(std::coroutine_handle<> handle)
        {
            channel->set_waiter(handle);
            channel->job() = {.buffer = &buffer,
                              .offset = channel->file().write_offset(),
                              .result = {.status = WriteStatus::kPending, .bytes_transferred = 0, .error_code = {}}};
            channel->arm();
            return true;
        }

        WriteResult await_resume() const noexcept
        {
            return channel->job().result;
        }
    };

    co_return co_await Awaiter{this, buffer};
}

void WriteChannel::on_event()
{
    if (!waiter_)
    {
        return;
    }

    if (job_.result.status == WriteStatus::kPending)
    {
        arm();
        return;
    }

    armed_ = false;
    auto waiter = std::exchange(waiter_, {});
    if (waiter && !waiter.done())
    {
        scheduler_.submit(waiter);
    }
}
} // namespace Foundation::Async
