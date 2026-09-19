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

    // Decides what the backend owes next. One accept covers one wait, so a
    // completion retires its wait as it answers it and there is nothing left to
    // wake here.
    void handle_completion();

    // Work the backend could take right now: there is a wait queued and none of
    // ours is with the kernel.
    bool has_prepared() const noexcept
    {
        return submitted_ == 0 && !pending_.empty();
    }

    // Hands the wait at the front over and answers one, or zero when there is
    // nothing to hand over.
    std::size_t count_prepared() noexcept;

    // The operation in flight reported its outcome.
    void complete_tasks(std::ptrdiff_t result) noexcept;

    // The wait the kernel is accepting for, or nothing when none is with it. Its
    // peer address is written in place when the operation is built.
    PendingAccept *submitted_front() noexcept
    {
        return submitted_ == 0 ? nullptr : &pending_.front();
    }

    // The connection a readiness backend took itself: it fills the result the
    // kernel did not, and the channel retires the wait like any other.
    void commit_result(Foundation::Core::AcceptResult accepted) noexcept;

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

    // A wait is prepared when it is awaited. Whether it also becomes submitted
    // right away is the backend's business, not the awaiter's.
    void prepare(Foundation::Async::Coroutine waiter, Foundation::Core::AcceptResult &result);

    // Arms or disarms the channel according to what the backend still owes.
    void refresh_arming() noexcept;

    // Answers the wait at the front and hands its waiter to the scheduler. One
    // accept covers one wait, so an answered wait is always finished.
    void wake_front() noexcept;

    // Leaves a waiting frame with an outcome, so that nothing is ever parked for a
    // completion that cannot come. Used when the channel goes away.
    void drop(PendingAccept &pending) noexcept;

    Foundation::Core::Socket listener_;
    std::deque<PendingAccept> pending_;
    // Whether the wait at the front has been handed to the kernel. One accept
    // covers one wait, so this is never more than one.
    std::size_t submitted_{0};
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
