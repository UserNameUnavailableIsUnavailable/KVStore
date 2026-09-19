#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/Address.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Socket.hpp>
#include <Foundation/NBIO/Types.hpp>
#include <deque>
#include <optional>
#include <system_error>
#include <utility>

namespace Foundation::NBIO
{
class AcceptAwaiter;

// One wait for a connection. The slot its outcome is written into belongs to the
// frame that is waiting, which is alive for exactly as long as this entry is
// queued, so the entry points at it rather than owning it.
class PendingAccept
{
  public:
    PendingAccept(Foundation::Async::Coroutine waiter, Foundation::Core::AcceptResult &result) noexcept
        : waiter_(std::move(waiter)), result_(&result)
    {
    }

    Foundation::Async::Coroutine &waiter() noexcept
    {
        return waiter_;
    }

    const Foundation::Async::Coroutine &waiter() const noexcept
    {
        return waiter_;
    }

    Foundation::Core::AcceptResult &result() noexcept
    {
        return *result_;
    }

    const Foundation::Core::AcceptResult &result() const noexcept
    {
        return *result_;
    }

  private:
    Foundation::Async::Coroutine waiter_;
    Foundation::Core::AcceptResult *result_{nullptr};
};

// Simplex channel dedicated to accepting. Taking a connection is not one operation
// that covers several waits the way a writev does, so the waits are taken one at a
// time and answered in the order they queued -- but one readiness event can mean
// several connections, which is why the backend may take more than one.
class AcceptChannel final : public Foundation::NBIO::Channel
{
  public:
    AcceptChannel(Foundation::Core::Socket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~AcceptChannel() noexcept;

    Foundation::NBIO::Task<std::optional<std::pair<Core::Socket, Core::Address>>> accept();

    // The batch protocol (see Channel.hpp). Taking a connection is not something
    // the kernel can vectorise, so one operation covers one wait: submit_jobs()
    // hands over the wait at the front of the queue and answers it, or nothing when
    // there is nobody waiting or a wait is already out there.
    PendingAccept *submit_jobs();

    // The backend's own outcome: the descriptor it accepted, or -errno. The peer
    // address was written into the wait when the operation was built, so it is
    // already in place.
    void advance_job(std::ptrdiff_t result) noexcept;

    // The connection a readiness backend took itself: the whole outcome arrives at
    // once, and the peer address did not come from the kernel.
    void advance_job(Foundation::Core::AcceptResult accepted) noexcept;

    void complete_jobs() noexcept;
    void handle_completion();

    Foundation::Core::Socket &socket() noexcept
    {
        return listener_;
    }
    const Foundation::Core::Socket &socket() const noexcept
    {
        return listener_;
    }

    std::error_code last_error() const noexcept
    {
        return error_code_;
    }

  private:
    friend class AcceptAwaiter;

    // Queues the wait and arms the channel: this is the suspension point, and being
    // armed is what tells the backend to look at the channel.
    void prepare(Foundation::Async::Coroutine waiter, Foundation::Core::AcceptResult &result);

    // Gives the wait at the front its answer and moves it to the completed queue.
    void retire_front(Foundation::Core::AcceptResult result) noexcept;

    // Leaves a waiting frame with an outcome, so that nothing is ever parked for a
    // completion that cannot come. Used when the channel goes away.
    void drop(PendingAccept &pending) noexcept;

    Foundation::Core::Socket listener_;
    // The queue, in the order the waits were awaited. One accept covers one wait, so
    // what is in `submitted_waits_` is at most the one wait the backend is
    // accepting for.
    std::deque<PendingAccept> prepared_waits_;
    std::deque<PendingAccept> submitted_waits_;
    std::deque<PendingAccept> completed_waits_;
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
