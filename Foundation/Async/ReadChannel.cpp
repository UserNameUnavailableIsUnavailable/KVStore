#include "ReadChannel.hpp"

#include "FileStream.hpp"

#include <cerrno>
#include <system_error>
#include <utility>

#include <unistd.h>

namespace Foundation::Async
{
namespace
{
ReadResult make_read_error()
{
    return {.status = ReadStatus::kError, .bytes_transferred = 0, .error_code = std::error_code(errno, std::system_category())};
}
} // namespace

ReadChannel::ReadChannel(FileStream &file, Multiplexer &multiplexer, Scheduler &scheduler)
    : Channel(ChannelType::kRead, file.native_handle(), multiplexer, scheduler), file_(file)
{
    multiplexer_.add_channel(this);
}

ReadChannel::~ReadChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

ReadResult ReadChannel::read_sync(Foundation::Core::Buffer &buffer)
{
    while (true)
    {
        const auto chunk = buffer.appendable_span();
        const auto result = ::pread(native_handle(), chunk.data(), chunk.size(), static_cast<off_t>(job_.offset));
        if (result < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return make_read_error();
        }
        if (result == 0)
        {
            return {.status = ReadStatus::kEndOfFile, .bytes_transferred = 0, .error_code = {}};
        }

        buffer.commit(static_cast<std::size_t>(result));
        job_.offset += static_cast<std::uint64_t>(result);
        file_.advance_read_offset(static_cast<std::size_t>(result));
        return {.status = ReadStatus::kDone,
                .bytes_transferred = static_cast<std::size_t>(result),
                .error_code = {}};
    }
}

Task<ReadResult> ReadChannel::read(Foundation::Core::Buffer &buffer)
{
    struct Awaiter
    {
        ReadChannel *channel;
        Foundation::Core::Buffer &buffer;

        bool await_ready() const noexcept
        {
            return false;
        }

        bool await_suspend(std::coroutine_handle<> handle)
        {
            channel->set_waiter(handle);
            channel->job() = {.buffer = &buffer,
                              .offset = channel->file().read_offset(),
                              .result = {.status = ReadStatus::kPending, .bytes_transferred = 0, .error_code = {}}};
            channel->arm();
            return true;
        }

        ReadResult await_resume() const noexcept
        {
            return channel->job().result;
        }
    };

    co_return co_await Awaiter{this, buffer};
}

void ReadChannel::on_event()
{
    if (!waiter_)
    {
        return;
    }

    if (job_.result.status == ReadStatus::kPending)
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
