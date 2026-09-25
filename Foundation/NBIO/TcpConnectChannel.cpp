#include "TcpConnectChannel.hpp"

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Payload.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <optional>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace Foundation::NBIO
{
TcpConnectChannel::TcpConnectChannel(Foundation::Core::TcpConnector connector, Foundation::NBIO::Multiplexer &multiplexer,
                                     Foundation::Async::Scheduler &scheduler) :
    // The parameter is built before the base is, so the socket it holds is what the
    // channel says it watches; the member takes it over from there.
    Channel(Foundation::NBIO::ChannelType::kConnect, static_cast<std::uintptr_t>(connector.native_handle()), multiplexer,
            scheduler),
    connector_(std::move(connector))
{
    if (!connector_.is_valid())
    {
        throw std::logic_error("invalid connector");
    }
    // A connect that blocks in the call would hold the whole engine for as long as the
    // handshake takes, so the socket is non-blocking from here: what it reports instead
    // is a connect under way, which is what this channel then waits for.
    if (auto result = connector_.non_blocking(true); !result)
    {
        throw std::system_error(result.error(), "non_blocking failed");
    }
    // Registered on the first arm(): nothing is watched until a connect queues.
}

TcpConnectChannel::~TcpConnectChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

class ConnectAwaiter
{
  public:
    ConnectAwaiter(TcpConnectChannel &channel, const Foundation::Core::SocketAddress *source,
                   const Foundation::Core::SocketAddress &target) :
        channel_(channel), source_(source), target_(target)
    {
    }

    ConnectAwaiter(const ConnectAwaiter &) = delete;
    ConnectAwaiter &operator=(const ConnectAwaiter &) = delete;

    // No cancellation hook. The parked Coroutine keeps this frame's control block
    // alive and the channel owns the waiter until it answers it, so a stale
    // registration is impossible rather than detected.

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType> bool await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        if (source_ != nullptr)
        {
            if (auto bound = channel_.connector_.bind(*source_); !bound)
            {
                failure_ = bound.error();
                return false;
            }
        }
        if (auto started = channel_.connector_.start_connect(target_); !started)
        {
            // Refused before the kernel took it -- an address that does not resolve, a
            // socket that is already connected -- so there is nothing to wait for.
            failure_ = started.error();
            return false;
        }
        channel_.park(Async::Coroutine::from_handle(handle));
        channel_.arm();
        return true;
    }

    Core::expected<Foundation::Core::TcpConnector, std::error_code> await_resume() noexcept
    {
        if (failure_.has_value())
        {
            return Core::unexpected<std::error_code>(*failure_);
        }
        // The wait is over, so the socket has the answer: made, or the reason it was
        // not. Either way it is the end of this channel's life, so the connection goes
        // back out with it.
        if (auto settled = channel_.connector_.finish_connect(); !settled)
        {
            return Core::unexpected<std::error_code>(settled.error());
        }
        return channel_.take_connector();
    }

  private:
    TcpConnectChannel &channel_;
    // Pointed at rather than held: the source address belongs to the caller, and the
    // awaiter outlives the call that named it.
    const Foundation::Core::SocketAddress *source_{nullptr};
    Foundation::Core::SocketAddress target_{};
    std::optional<std::error_code> failure_{};
};

// The connect bodies, as plain coroutines whose channel is an ordinary parameter:
// the implicit object parameter of a member coroutine is laid out where the promise
// lives, so `*this` inside the body comes back as the inherited control block instead
// of the channel. Keeping it a parameter keeps it in the parameter area.
static Foundation::NBIO::Task<Core::expected<Foundation::Core::TcpConnector, std::error_code>> ConnectOn(
    TcpConnectChannel &channel, const Foundation::Core::SocketAddress *source, const Foundation::Core::SocketAddress &target)
{
    co_return co_await ConnectAwaiter{channel, source, target};
}

void TcpConnectChannel::complete()
{
    auto &payload = std::get<ConnectPayload>(payload_);
    payload.release_poll();

    // The socket stays writable once it is, so a registration left in place would
    // report readiness on every pass from here on. A connection is made once: this is
    // the end of the wait, not a pause in it.
    disarm();

    if (!waiter_) [[unlikely]]
    {
        return;
    }
    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}

Payload &TcpConnectChannel::submit()
{
    return payload_;
}

Foundation::NBIO::Task<Core::expected<Foundation::Core::TcpConnector, std::error_code>> TcpConnectChannel::connect(
    const Foundation::Core::SocketAddress &target)
{
    return ConnectOn(*this, nullptr, target);
}

Foundation::NBIO::Task<Core::expected<Foundation::Core::TcpConnector, std::error_code>> TcpConnectChannel::connect(
    const Foundation::Core::SocketAddress &source, const Foundation::Core::SocketAddress &target)
{
    return ConnectOn(*this, &source, target);
}
} // namespace Foundation::NBIO
