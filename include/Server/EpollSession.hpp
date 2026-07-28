#pragma once

#include <coroutine>
#include <exception>
#include <utility>

#include "Common/Session.hpp"

namespace KV
{
class EpollSession final : public Session
{
public:
    struct promise_type
    {
        std::exception_ptr exception;

        EpollSession get_return_object() noexcept
        {
            return EpollSession(Handle::from_promise(*this));
        }

        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}

        void unhandled_exception() noexcept
        {
            exception = std::current_exception();
        }
    };

    using Handle = std::coroutine_handle<promise_type>;

    EpollSession() : Session(NetworkingModel::kReactor) {}
    EpollSession(const EpollSession&) = delete;
    EpollSession& operator=(const EpollSession&) = delete;
    EpollSession(EpollSession&& other) noexcept :
        Session(std::move(other)),
        handle_(std::exchange(other.handle_, nullptr))
    {
    }

    // Session (the base) disables move assignment, so EpollSession is
    // move-constructible but not move-assignable. Coroutine ownership is
    // transferred explicitly via AdoptCoroutine instead.
    EpollSession& operator=(EpollSession&&) = delete;

    ~EpollSession() noexcept
    {
        Reset();
    }

    void AdoptCoroutine(EpollSession&& coroutine)
    {
        if (handle_)
        {
            handle_.destroy();
        }
        handle_ = std::exchange(coroutine.handle_, nullptr);
    }

    void Resume() const
    {
        if (handle_ && !handle_.done())
        {
            handle_.resume();
        }
    }

    bool Done() const
    {
        return !handle_ || handle_.done();
    }

    void RethrowIfFailed() const
    {
        if (handle_ && handle_.promise().exception)
        {
            std::rethrow_exception(handle_.promise().exception);
        }
    }

    void Reset()
    {
        if (handle_)
        {
            handle_.destroy();
            handle_ = nullptr;
        }
        Session::Reset();
    }

private:
    explicit EpollSession(Handle handle) noexcept :
        Session(NetworkingModel::kReactor), handle_(handle)
    {
    }

    Handle handle_ = nullptr;
};
} // namespace KV