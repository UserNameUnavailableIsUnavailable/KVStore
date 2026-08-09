#pragma once

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <coroutine>
#include <span>
#include <string_view>
#include <utility>

#include "Common/Message.hpp"
#include "Common/Socket.hpp"
#include "Common/Session.hpp"
#include "Common/TimerQueue.hpp"

namespace KV
{
class Session;

class SessionTask
{
public:
    class promise_type
    {
    public:
        SessionTask get_return_object() noexcept;
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}

        // I/O errors never throw — the awaiter returns a sentinel value (empty
        // span / 0) and the coroutine exits via co_return.  If an exception
        // still reaches here it is a genuine bug (logic error, bad_alloc, …);
        // we crash loudly instead of silently swallowing it.
        void unhandled_exception() noexcept;
    };

    using Handle = std::coroutine_handle<promise_type>;

    explicit SessionTask(Handle handle) noexcept : handle_(handle) {}
    SessionTask(const SessionTask&) = delete;
    SessionTask& operator=(const SessionTask&) = delete;
    SessionTask(SessionTask&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}
    SessionTask& operator=(SessionTask&& other) noexcept;
    ~SessionTask() noexcept;

    bool Done() const noexcept;

private:
    Handle handle_ = nullptr;
};

class ReceiveOperation
{
public:
    ReceiveOperation(MessageQueue& queue, Session& session);

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> continuation);
    std::span<const std::byte> await_resume();

    Socket::HandleType GetHandle() const noexcept
    {
        return session_.GetSocket().GetNativeHandle();
    }
    std::span<char> GetBuffer() const noexcept { return buffer_; }
    std::coroutine_handle<> GetContinuation() const noexcept { return continuation_; }
    void Complete(int result) noexcept { result_ = result; }

private:
    MessageQueue& queue_;
    Session& session_;
    std::span<char> buffer_;
    int result_ = 0;
    std::coroutine_handle<> continuation_;
};

class SendOperation
{
public:
    SendOperation(MessageQueue& queue, Session& session);

    bool await_ready() const noexcept { return buffer_.empty(); }
    void await_suspend(std::coroutine_handle<> continuation);
    std::size_t await_resume();

    Socket::HandleType GetHandle() const noexcept
    {
        return session_.GetSocket().GetNativeHandle();
    }
    std::span<const char> GetBuffer() const noexcept { return buffer_; }
    std::coroutine_handle<> GetContinuation() const noexcept { return continuation_; }
    void Complete(int result) noexcept { result_ = result; }

private:
    MessageQueue& queue_;
    Session& session_;
    std::span<const char> buffer_;
    int result_ = 0;
    std::coroutine_handle<> continuation_;
};

class ConnectOperation
{
public:
    ConnectOperation(MessageQueue& queue, Session& session, std::string_view host, std::uint16_t port);

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> continuation);
    bool await_resume() const noexcept { return result_ == 0; }

    Socket::HandleType GetHandle() const noexcept
    {
        return session_.GetSocket().GetNativeHandle();
    }
    const ::sockaddr_in& GetAddress() const noexcept { return address_; }
    std::coroutine_handle<> GetContinuation() const noexcept { return continuation_; }
    void Complete(int result) noexcept
    {
        result_ = result;
        registered_ = false;
    }
    bool IsRegistered() const noexcept { return registered_; }
    void SetRegistered(bool value) noexcept { registered_ = value; }

    ~ConnectOperation() noexcept;

private:
    MessageQueue& queue_;
    Session& session_;
    ::sockaddr_in address_ {};
    int result_ = -EINVAL;
    bool registered_ = false;
    std::coroutine_handle<> continuation_;
};

// DelayedOperation suspends the awaiting coroutine until a deadline passes.
//
//     co_await DelayedOperation(queue, std::chrono::seconds(1));
//
// It always suspends: a delay that returned inline would not be a delay, and
// the message loop would never get a chance to run.
//
// The operation holds no timer of its own -- it is one node in the queue's
// deadline heap, so a thousand concurrent waits still cost a single timerfd.
// Registering is O(log n) and touches the kernel only when this wait becomes
// the earliest one outstanding.
class DelayedOperation
{
public:
    using Clock = TimerQueue::Clock;

    template <typename Rep, typename Period>
    DelayedOperation(MessageQueue& queue, std::chrono::duration<Rep, Period> delay) :
        queue_(queue),
        // The deadline is fixed when the co_await expression is evaluated, not
        // when the queue gets around to registering it, so scheduling latency
        // cannot stretch the requested delay.
        deadline_(Clock::now() + std::chrono::duration_cast<Clock::duration>(delay))
    {
    }

    DelayedOperation(const DelayedOperation&) = delete;
    DelayedOperation& operator=(const DelayedOperation&) = delete;
    DelayedOperation(DelayedOperation&&) = delete;
    DelayedOperation& operator=(DelayedOperation&&) = delete;

    // Withdraw from the queue if this operation is torn down while still
    // scheduled, which is what happens when the awaiting coroutine's frame is
    // destroyed before the deadline arrives.  Without this the heap would keep a
    // continuation pointing into freed memory.
    ~DelayedOperation() noexcept;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> continuation);

    // True when the delay elapsed, false when the wait was cut short.  Like the
    // I/O operations this never throws: a cancelled timer is an expected
    // outcome, not an error.
    bool await_resume() const noexcept { return elapsed_; }

    Clock::time_point GetDeadline() const noexcept { return deadline_; }
    std::coroutine_handle<> GetContinuation() const noexcept { return continuation_; }

    void SetToken(TimerQueue::Token token) noexcept { token_ = token; }
    TimerQueue::Token GetToken() const noexcept { return token_; }

    // Called by the queue when the deadline passes.
    void Complete(bool elapsed) noexcept
    {
        elapsed_ = elapsed;
        token_ = {};
    }

private:
    MessageQueue& queue_;
    Clock::time_point deadline_;
    TimerQueue::Token token_;
    bool elapsed_ = false;
    std::coroutine_handle<> continuation_;
};

// DetachedTask is a fire-and-forget coroutine: it owns itself, and its frame is
// released by the coroutine machinery as soon as the body finishes.  That is
// what makes a JavaScript-style setTimeout possible -- the caller does not have
// to keep a handle alive just to see the callback run.
//
// Caveat: because nobody holds the frame, a detached task that never resumes
// (its queue was destroyed while the timer was pending) leaks its frame.  Only
// schedule detached work on a queue that outlives it.
class DetachedTask
{
public:
    struct promise_type
    {
        DetachedTask get_return_object() const noexcept { return {}; }
        std::suspend_never initial_suspend() const noexcept { return {}; }
        std::suspend_never final_suspend() const noexcept { return {}; }
        void return_void() const noexcept {}
        void unhandled_exception() const noexcept;
    };
};

// Run *callback* once, *delay* from now.
//
//     SetTimeout(queue, std::chrono::milliseconds(500), [] { Reconnect(); });
template <typename Rep, typename Period, typename Callable>
DetachedTask SetTimeout(MessageQueue& queue, std::chrono::duration<Rep, Period> delay, Callable callback)
{
    co_await DelayedOperation(queue, delay);
    callback();
}

// Run *callback* every *interval* until it returns false.
//
//     SetInterval(queue, std::chrono::seconds(1), [this] { return Synchronize(); });
//
// Each tick uses a fresh timer rather than a periodic one, which costs two
// extra syscalls per tick but keeps the interval measured from the moment the
// previous callback returned -- a slow callback can never queue up a backlog of
// expirations it then has to run back to back.
template <typename Rep, typename Period, typename Callable>
DetachedTask SetInterval(MessageQueue& queue, std::chrono::duration<Rep, Period> interval, Callable callback)
{
    while (true)
    {
        co_await DelayedOperation(queue, interval);
        if (!callback())
        {
            co_return;
        }
    }
}
} // namespace KV
