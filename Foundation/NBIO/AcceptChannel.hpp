#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/Address.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Socket.hpp>
#include <deque>
#include <system_error>

namespace Foundation::NBIO
{
// One wait for a connection. The slot its outcome goes into belongs to the frame
// that is parked, which is also where the kernel writes the peer address when the
// accept is submitted.
struct PendingAccept
{
    Foundation::Core::AcceptResult *result{nullptr};
    Foundation::Async::Coroutine waiter;
};

class AcceptChannel final : public Foundation::NBIO::Channel
{
  public:
    AcceptChannel(Foundation::Core::Socket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~AcceptChannel() noexcept override;
    void handle_event() override;

    Foundation::NBIO::Task<std::optional<std::pair<Core::Socket, Core::Address>>> accept();

    // Hands a wait for a connection to the channel. Taking connections is not one
    // operation that covers several of them the way a writev does, so they are
    // taken one at a time and answered in the order they queued -- but a readiness
    // event can satisfy more than one, which is what flush() does.
    void submit(Foundation::Core::AcceptResult &result, Async::Coroutine waiter);

    // The wait at the front of the queue: the one the kernel is accepting for.
    Foundation::Core::AcceptResult *front() noexcept
    {
        return pending_.empty() ? nullptr : pending_.front().result;
    }

    // What one accept returned: an accepted socket, a "not yet", or an error.
    void accepted(int result) noexcept;

    // Takes connections while there are connections to take and someone waiting for
    // them. A readiness multiplexer drives this itself; a completion multiplexer
    // gets its answers one completion at a time instead.
    void flush() noexcept;

    const Foundation::Core::Socket &socket() const noexcept
    {
        return socket_;
    }
    Foundation::Core::Socket &socket() noexcept
    {
        return socket_;
    }

    std::error_code last_error() const noexcept
    {
        return error_code_;
    }

  private:
    // Puts the wait at the front of the queue in front of the kernel.
    void arm_next();

    void fail(PendingAccept &pending) noexcept;

    Foundation::Core::Socket socket_;
    std::deque<PendingAccept> pending_;
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
