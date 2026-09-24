#include "TcpAcceptChannel.hpp"
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Core/TcpSocket.hpp>
#include <optional>
#include <stdexcept>
#include <utility>

#include <Foundation/Async/Scheduler.hpp>

namespace Foundation::NBIO
{
TcpAcceptChannel::TcpAcceptChannel(Foundation::Core::TcpSocket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Channel(Foundation::NBIO::ChannelType::kAccept, static_cast<std::uintptr_t>(socket.native_handle()), multiplexer, scheduler), listener_(std::move(socket))
{
    if (!listener_.is_valid())
    {
        throw std::logic_error("invalid socket");
    }
    if (auto result = listener_.non_blocking(true); !result)
    {
        throw std::system_error(result.error(), "non_blocking failed");
    }
    // Registered on the first arm(): there is nothing to watch until a wait queues.
}

TcpAcceptChannel::~TcpAcceptChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

class AcceptAwaiter
{
  public:
    AcceptAwaiter(TcpAcceptChannel &channel) : channel_(channel)
    {
    }
    AcceptAwaiter(const AcceptAwaiter &) = delete;
    AcceptAwaiter &operator=(const AcceptAwaiter &) = delete;

    ~AcceptAwaiter() noexcept = default;

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType> void await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        channel_.prepare(Async::Coroutine::from_handle(handle), &communication_);
        channel_.arm();
    }

    Core::Communication await_resume() noexcept
    {
        return std::move(communication_);
    }

  private:
    TcpAcceptChannel &channel_;
    Core::Communication communication_{};
};

// The accept body, as a plain coroutine whose channel is an ordinary parameter
// (see the note on TcpAcceptChannel::accept).
static Foundation::NBIO::Task<Core::expected<std::pair<Core::TcpSocket, Core::SocketAddress>, std::error_code>> AcceptOn(TcpAcceptChannel &channel)
{
    auto result = co_await AcceptAwaiter(channel);
    if (result.status == Core::OperationStatus::kError)
    {
        co_return Core::unexpected<std::error_code>(std::move(result.error_code));
    }
    co_return std::make_pair(std::move(result.socket), std::move(result.address));
}

void TcpAcceptChannel::prepare(Async::Coroutine waiter, Core::Communication *communication)
{
    communication->status = Core::OperationStatus::kPending;
    communication->socket = {};
    communication->address = {};
    communication->address.length() = Core::SocketAddress::capacity();
    communication->error_code = {};
    waiters_.emplace_back(std::move(waiter));
    auto &payload = std::get<AcceptPayload>(payload_);
    payload.submit(communication);
}

Payload &TcpAcceptChannel::submit()
{
    return payload_;
}

void TcpAcceptChannel::complete()
{
    auto &payload = std::get<AcceptPayload>(payload_);
    while (auto next = payload.next_completion())
    {
        auto waiter = std::move(waiters_.front());
        waiters_.pop_front();
        scheduler_.submit(std::move(waiter));
        payload.conclude();
    }
    if (payload.size() != 0)
    {
        arm();
    }
    else
    {
        disarm();
    }
}

Foundation::NBIO::Task<Core::expected<std::pair<Core::TcpSocket, Core::SocketAddress>, std::error_code>> TcpAcceptChannel::accept()
{
    // Deliberately not a member coroutine: the implicit object parameter of a
    // member coroutine is laid out by the compiler in the same frame slot the
    // promise uses, so `*this` inside the body came back as the inherited
    // control block instead of the channel. Taking the channel as an ordinary
    // parameter keeps it in the parameter area.
    return AcceptOn(*this);
}
} // namespace Foundation::NBIO
