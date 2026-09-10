#pragma once

#include <coroutine>
#include <cstddef>
#include <limits>

namespace KV
{
class ReceiveOperation;
class SendOperation;
class ConnectOperation;
class DelayedOperation;

enum class MessageType
{
    kAccept,
    kRead,
    kWrite,
    kConnect,
    kTimer,
};

struct Message
{
    // A timer is not attached to a socket, so timer messages carry this instead
    // of a session id.  Dispatchers must route them by continuation, not by
    // looking up a session.
    static constexpr std::size_t kNoSession = std::numeric_limits<std::size_t>::max();

    MessageType type;
    std::size_t session_id;
    int result = 0;
    std::coroutine_handle<> continuation;
};

class MessageQueue
{
public:
    virtual ~MessageQueue() = default;
    virtual void RegisterRead(ReceiveOperation& task) = 0;
    virtual void RegisterWrite(SendOperation& task) = 0;
    // Returns false when connect completed synchronously; true when the
    // coroutine must remain suspended.
    virtual bool RegisterConnect(ConnectOperation& task) = 0;
    virtual void cancelConnect(ConnectOperation& task) noexcept = 0;

    // Put the operation's deadline into the backend's timer heap.
    virtual void RegisterTimer(DelayedOperation& task) = 0;

    // Withdraw a scheduled timer.  An operation whose coroutine frame is
    // destroyed while it is still waiting has to call this, or the heap would
    // keep a continuation pointing into freed memory.
    virtual void cancelTimer(DelayedOperation& task) noexcept = 0;

    virtual Message wait() = 0;
};
} // namespace KV
