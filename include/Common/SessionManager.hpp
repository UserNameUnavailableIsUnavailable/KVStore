#pragma once

#include <cstddef>
#include <optional>
#include <utility>

#include "Common/Slab.hpp"

namespace KV
{
// Phase of a session's life. There is no "closed" phase: a session that has
// finished draining is released, so its handle simply stops resolving.
enum class SessionState
{
    kActive, // serving normally; new I/O may be submitted
    kDraining // close requested; no new I/O, waiting for submitted operations to settle
};

// -------- SessionManager --------
// Owns every live session and decides when one may be destroyed.
//
// Slab<T> answers "where is the object for this handle, and is the handle still
// current". That is a memory question. It says nothing about whether destroying
// a particular session right now is *safe*, and in an event-driven server it
// usually is not:
//
//   - io_uring may hold a pointer to a buffer that lives inside the session,
//     handed to the kernel when the read was submitted;
//   - the awaiter that will receive the completion lives inside the coroutine
//     frame, which the session's task owns;
//   - a cancellation is not effective when it is submitted, only when it is
//     acknowledged by a completion.
//
// So closing a session is a process, not an instant. This class runs it:
// BeginClose() moves the session to kDraining and stops it being handed out,
// each settled operation is reported back, and only when the last one has
// settled is the slot actually released - destroying the session, its buffers
// and its coroutine frame together, at a point where nothing can still refer to
// them. A generation bump then makes every handle minted for it fail to resolve,
// so a completion that arrives even later is quietly ignored.
//
// The manager never touches TaskType beyond moving and destroying it, and never
// touches SessionType at all. What a session *does* is not its concern; when a
// session may cease to exist is.
//
// Not thread safe: one manager belongs to one event loop.
template <typename SessionType, typename TaskType, std::size_t SlotsPerBlock = 512>
class SessionManager
{
    struct Entry
    {
        template <typename... Args>
        explicit Entry(Args&&... args) :
            session(std::forward<Args>(args)...)
        {
        }

        SessionType session;
        // Engaged once the session has been started. Destroying it destroys the
        // coroutine frame, which is why it may only happen once no submitted
        // operation can still reach the awaiters living in that frame.
        std::optional<TaskType> task;
        SessionState state = SessionState::kActive;
        // Operations handed to the kernel (or to epoll) that have not yet come
        // back, whether they will come back as success, error or cancellation.
        std::size_t pending_operations = 0;
    };

    using EntrySlab = Slab<Entry, SlotsPerBlock>;

public:
    // Scoped to this manager's session type, so a session handle cannot be
    // mistaken for a handle into some other pool.
    using Handle = typename EntrySlab::Handle;

    SessionManager() = default;

    SessionManager(const SessionManager&) = delete;
    SessionManager& operator=(const SessionManager&) = delete;
    SessionManager(SessionManager&&) noexcept = default;
    SessionManager& operator=(SessionManager&&) noexcept = default;

    // -------- Opening --------

    // Construct a session in place from its own constructor arguments. It starts
    // out kActive but without a task; call Start() with the coroutine that will
    // serve it.
    //
    // Session creation is deliberately two-phase: the coroutine needs the
    // session's address, so it cannot be built until the session exists. The
    // address handed back here is stable for the session's whole life, so the
    // coroutine may capture it.
    template <typename... Args>
    std::pair<Handle, SessionType&> Acquire(Args&&... args)
    {
        auto [handle, entry] = slab_.Acquire(std::forward<Args>(args)...);
        return {handle, entry.session};
    }

    // Hand the session the coroutine that serves it. Returns false if the handle
    // has expired, in which case the task is dropped - and with it the coroutine
    // frame, before it can submit anything.
    bool Start(Handle handle, TaskType&& task) noexcept
    {
        Entry* entry = slab_.Find(handle);
        if (entry == nullptr)
        {
            return false;
        }
        entry->task.emplace(std::move(task));
        return true;
    }

    // -------- Lookup --------

    // The session behind a handle, or nullptr when it is gone or draining.
    // Draining sessions are withheld on purpose: nothing new should be started
    // on a session that is on its way out.
    SessionType* Find(Handle handle) noexcept
    {
        Entry* entry = slab_.Find(handle);
        return entry != nullptr && entry->state == SessionState::kActive ? &entry->session : nullptr;
    }

    const SessionType* Find(Handle handle) const noexcept
    {
        const Entry* entry = slab_.Find(handle);
        return entry != nullptr && entry->state == SessionState::kActive ? &entry->session : nullptr;
    }

    // The task serving a session, so the caller can ask whether it has finished.
    // The manager itself never inspects it.
    TaskType* GetTask(Handle handle) noexcept
    {
        Entry* entry = slab_.Find(handle);
        return entry != nullptr && entry->task.has_value() ? &*entry->task : nullptr;
    }

    // -------- Closing --------

    // Request that a session be closed. Idempotent, so every path that notices a
    // session is finished - EOF, protocol error, I/O error, a task that ran to
    // completion, server shutdown - can just call it.
    //
    // Releases the session immediately when nothing is outstanding; otherwise it
    // becomes invisible to Find() and is released by the last OnOperationSettled.
    // Returns true if the session was released before returning, which tells the
    // caller its reference is now dangling.
    bool BeginClose(Handle handle) noexcept
    {
        Entry* entry = slab_.Find(handle);
        if (entry == nullptr)
        {
            return false;
        }

        entry->state = SessionState::kDraining;
        return ReleaseIfDrained(handle, *entry);
    }

    // Request that every live session be closed. Sessions with nothing
    // outstanding go away at once; the rest drain.
    void CloseAll() noexcept
    {
        slab_.ForEach([this](Handle handle, Entry&) { BeginClose(handle); });
    }

    // -------- Outstanding operations --------
    // The manager cannot see submissions or completions, so it has to be told
    // about them. These two calls are what make the release point correct.

    // An operation was handed to the kernel. Returns false if the handle has
    // expired or the session is draining, meaning the caller must not submit.
    bool OnOperationSubmitted(Handle handle) noexcept
    {
        Entry* entry = slab_.Find(handle);
        if (entry == nullptr || entry->state != SessionState::kActive)
        {
            return false;
        }
        ++entry->pending_operations;
        return true;
    }

    // An operation came back - completed, failed, or acknowledged as cancelled.
    //
    // Returns the session when the event should still be acted on, or nullptr
    // when it should be discarded because the session has expired or is
    // draining. Either way the outstanding count has been updated, so a caller
    // that ignores the return value still cannot strand a draining session. When
    // this was the last outstanding operation of a draining session, the session
    // has been released by the time this returns.
    SessionType* OnOperationSettled(Handle handle) noexcept
    {
        Entry* entry = slab_.Find(handle);
        if (entry == nullptr)
        {
            return nullptr; // a completion from an earlier occupant of the slot
        }

        if (entry->pending_operations > 0)
        {
            --entry->pending_operations;
        }

        if (entry->state == SessionState::kActive)
        {
            return &entry->session;
        }

        ReleaseIfDrained(handle, *entry);
        return nullptr;
    }

    // -------- Observers --------

    bool Contains(Handle handle) const noexcept { return slab_.Contains(handle); }

    bool IsActive(Handle handle) const noexcept
    {
        const Entry* entry = slab_.Find(handle);
        return entry != nullptr && entry->state == SessionState::kActive;
    }

    bool IsDraining(Handle handle) const noexcept
    {
        const Entry* entry = slab_.Find(handle);
        return entry != nullptr && entry->state == SessionState::kDraining;
    }

    std::size_t GetPendingOperationCount(Handle handle) const noexcept
    {
        const Entry* entry = slab_.Find(handle);
        return entry != nullptr ? entry->pending_operations : 0;
    }

    // Sessions held, draining ones included.
    std::size_t Size() const noexcept { return slab_.Size(); }
    bool Empty() const noexcept { return slab_.Empty(); }
    std::size_t Capacity() const noexcept { return slab_.Capacity(); }

    std::size_t GetActiveCount() const noexcept { return CountIn(SessionState::kActive); }
    std::size_t GetDrainingCount() const noexcept { return CountIn(SessionState::kDraining); }

    // Visit every active session. Draining sessions are skipped, matching
    // Find(). Closing the visited session from inside the callback is allowed;
    // opening a new one is not.
    template <typename Visitor>
    void ForEachActive(Visitor&& visit)
    {
        slab_.ForEach([&visit](Handle handle, Entry& entry) {
            if (entry.state == SessionState::kActive)
            {
                visit(handle, entry.session);
            }
        });
    }

private:
    // Release a draining session once nothing can still refer to it. This is the
    // single place a session is destroyed, and the only place that decides it is
    // safe to do so.
    bool ReleaseIfDrained(Handle handle, Entry& entry) noexcept
    {
        if (entry.state != SessionState::kDraining || entry.pending_operations > 0)
        {
            return false;
        }
        // Destroys the session, its buffers and its coroutine frame together, and
        // bumps the slot's generation so no later completion can resolve to it.
        return slab_.Release(handle);
    }

    std::size_t CountIn(SessionState state) const noexcept
    {
        std::size_t count = 0;
        slab_.ForEach([&count, state](Handle, const Entry& entry) {
            if (entry.state == state)
            {
                ++count;
            }
        });
        return count;
    }

    EntrySlab slab_;
};
} // namespace KV
