#include "Task.hpp"

#include <arpa/inet.h>
#include <cstdio>
#include <exception>
#include <string>

#include "Message.hpp"
#include "Session.hpp"

namespace KV
{
SessionTask SessionTask::promise_type::get_return_object() noexcept
{
    return SessionTask(Handle::from_promise(*this));
}

void SessionTask::promise_type::unhandled_exception() noexcept
{
    // Genuine bugs only — I/O errors are handled via return values.
    try
    {
        std::rethrow_exception(std::current_exception());
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "coroutine panic: %s\n", e.what());
    }
    catch (...)
    {
        std::fprintf(stderr, "coroutine panic: unknown exception\n");
    }
    std::terminate();
}

SessionTask& SessionTask::operator=(SessionTask&& other) noexcept
{
    if (this != &other)
    {
        if (handle_)
        {
            handle_.destroy();
        }
        handle_ = std::exchange(other.handle_, nullptr);
    }
    return *this;
}

SessionTask::~SessionTask() noexcept
{
    if (handle_)
    {
        handle_.destroy();
    }
}

bool SessionTask::Done() const noexcept
{
    return !handle_ || handle_.done();
}

ReceiveOperation::ReceiveOperation(MessageQueue& queue, Session& session) :
    queue_(queue), session_(session), buffer_(session.PrepareReceive())
{
}

void ReceiveOperation::await_suspend(std::coroutine_handle<> continuation)
{
    continuation_ = continuation;
    queue_.RegisterRead(*this);
}

std::span<const std::byte> ReceiveOperation::await_resume()
{
    // I/O errors are expected (client disconnect, RST, timeout) — never throw.
    // Return an empty span; the coroutine treats it as EOF and exits via
    // co_return, then the dispatch loop notices Done() and closes the session.
    if (result_ < 0)
    {
        session_.CompleteReceive(0);
        return {};
    }
    return session_.CompleteReceive(result_);
}

SendOperation::SendOperation(MessageQueue& queue, Session& session) :
    queue_(queue), session_(session), buffer_(session.GetPendingSend())
{
}

void SendOperation::await_suspend(std::coroutine_handle<> continuation)
{
    continuation_ = continuation;
    queue_.RegisterWrite(*this);
}

std::size_t SendOperation::await_resume()
{
    if (result_ < 0)
    {
        session_.CompleteSend(0);
        return 0;
    }
    session_.CompleteSend(result_);
    return static_cast<std::size_t>(result_);
}

ConnectOperation::ConnectOperation(MessageQueue& queue, Session& session,
    std::string_view host, std::uint16_t port) :
    queue_(queue), session_(session)
{
    address_.sin_family = AF_INET;
    address_.sin_port = htons(port);
    if (::inet_pton(AF_INET, std::string(host).c_str(), &address_.sin_addr) != 1)
    {
        result_ = -EINVAL;
    }
    else
    {
        result_ = -EINPROGRESS;
    }
}

bool ConnectOperation::await_suspend(std::coroutine_handle<> continuation)
{
    continuation_ = continuation;
    if (result_ == -EINVAL)
    {
        return false;
    }
    registered_ = queue_.RegisterConnect(*this);
    return registered_;
}

ConnectOperation::~ConnectOperation() noexcept
{
    if (registered_)
    {
        queue_.cancelConnect(*this);
    }
}

DelayedOperation::~DelayedOperation() noexcept
{
    // A live token means the deadline never arrived, so the queue still holds a
    // continuation into a frame that is going away.
    if (token_.IsValid())
    {
        queue_.cancelTimer(*this);
    }
}

void DelayedOperation::await_suspend(std::coroutine_handle<> continuation)
{
    continuation_ = continuation;
    queue_.RegisterTimer(*this);
}

void DetachedTask::promise_type::unhandled_exception() const noexcept
{
    // A detached task has no owner to hand an exception to, so the policy is the
    // same as for a session task: an escaping exception is a bug, not an
    // expected outcome.
    try
    {
        std::rethrow_exception(std::current_exception());
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "detached coroutine panic: %s\n", e.what());
    }
    catch (...)
    {
        std::fprintf(stderr, "detached coroutine panic: unknown exception\n");
    }
    std::terminate();
}
} // namespace KV
