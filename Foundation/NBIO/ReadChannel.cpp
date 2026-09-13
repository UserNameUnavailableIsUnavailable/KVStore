#include "ReadChannel.hpp"
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
struct ReadAwaiter
{
    ReadChannel *channel;
    Foundation::Core::Buffer &buffer;

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        auto coroutine = Async::Coroutine::from_handle(handle);
        channel->arm();
        channel->park(std::move(coroutine));
        channel->job() = {.buffer = &buffer,
                            .offset = channel->file_stream().read_offset(),
                            .result = {.status = Foundation::Core::ReadStatus::kPending, .bytes_transferred = 0, .error_code = {}}};
        return true;
    }

    Foundation::Core::ReadResult await_resume() const noexcept
    {
        return channel->job().result;
    }
};

Foundation::Core::ReadResult make_read_error()
{
    return {.status = Foundation::Core::ReadStatus::kError, .bytes_transferred = 0, .error_code = std::error_code(errno, std::system_category())};
}
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

Foundation::Core::ReadResult ReadChannel::read_sync(Foundation::Core::Buffer &buffer)
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
            return {.status = Foundation::Core::ReadStatus::kEndOfFile, .bytes_transferred = 0, .error_code = {}};
        }

        buffer.commit(static_cast<std::size_t>(result));
        job_.offset += static_cast<std::uint64_t>(result);
        file_.advance_read_offset(static_cast<std::size_t>(result));
        return {.status = Foundation::Core::ReadStatus::kDone,
                .bytes_transferred = static_cast<std::size_t>(result),
                .error_code = {}};
    }
}

Foundation::NBIO::Task<Foundation::Core::ReadResult> ReadChannel::read(Foundation::Core::Buffer &buffer)
{

    co_return co_await ReadAwaiter{this, buffer};
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
