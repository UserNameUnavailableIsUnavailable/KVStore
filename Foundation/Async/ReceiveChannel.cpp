#include "ReceiveChannel.hpp"

#include <spdlog/spdlog.h>

#include <Foundation/Socket.hpp>
#include <cassert>
#include <stdexcept>
#include <utility>

#include "Task.hpp"

namespace Foundation::Async
{
ReceiveChannel::ReceiveChannel(Foundation::Socket &socket, Multiplexer &multiplexer, Scheduler &scheduler)
    : Channel(ChannelType::kReceive, socket.get_native_handle(), multiplexer, scheduler), socket_(socket)
{
    if (!socket.is_valid())
    {
        throw std::logic_error("invalid socket");
    }
    socket_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

ReceiveChannel::~ReceiveChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void ReceiveChannel::on_event()
{
    if (!waiter_)
    {
        // Nobody is waiting; the event is not ours to consume.
        spdlog::warn("Event is triggered but no one is waiting for it, ignored.");
        return;
    }

    // Readiness backends install a handler that performs the receive. Completion
    // backends leave it unset: the kernel already transferred the data and the
    // multiplexer wrote the outcome into the job before calling us.
    if (handler_ != nullptr)
    {
        handler_(this);
    }

    if (job_.result.status == ReceiveStatus::kPending)
    {
        // Not finished (e.g. EAGAIN): stay armed and keep the coroutine suspended.
        arm();
        return;
    }

    // Finished. The multiplexer cleared the armed flag before dispatching
    // (one-shot semantics), so keep it down; it syncs the kernel registration
    // once, after we return.
    armed_ = false;
    auto waiter = std::exchange(waiter_, {});
    if (waiter && !waiter.done())
    {
        scheduler_.submit(waiter);
    }
}

Task<ReceiveResult> ReceiveChannel::receive(::Foundation::Buffer &buffer)
{
    auto awaiter = detail::ReceiveAwaiter(this, buffer);
    auto result = co_await std::move(awaiter);
    co_return std::move(result);
}
} // namespace Foundation::Async
