#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory_resource>
#include <optional>
#include <unordered_map>
#include <variant>
#include <vector>

#include <liburing.h>

#include "IOUringSession.hpp"
#include "Message.hpp"
#include "Task.hpp"
#include "TimerQueue.hpp"
#include "Server/Server.hpp"

#if not defined (__linux__)
#error "This header is linux-specific."
#endif

namespace KV
{
class IOUringMessageQueue final : public MessageQueue
{
public:
    IOUringMessageQueue(std::size_t submission_capacity, std::size_t completion_capacity);
    ~IOUringMessageQueue() noexcept override;

    void PrepareAccept(std::size_t session_id, Socket::HandleType listener,
        Address& address);
    void RegisterRead(ReceiveOperation& task) override;
    void RegisterWrite(SendOperation& task) override;
    bool RegisterConnect(ConnectOperation& task) override;
    void cancelConnect(ConnectOperation& task) noexcept override;
    void RegisterTimer(DelayedOperation& task) override;
    void cancelTimer(DelayedOperation& task) noexcept override;
    Message wait() override;

private:
    using PendingTask = std::variant<ReceiveOperation*, SendOperation*, ConnectOperation*>;

    io_uring_sqe& GetSubmission();
    void Submit();
    void PrepareRead(std::uint64_t id, ReceiveOperation& task);
    void PrepareWrite(std::uint64_t id, SendOperation& task);
    void PrepareConnect(std::uint64_t id, ConnectOperation& task);
    // Watch the timer heap's fd for readability.
    void PollTimerQueue();
    void CollectExpiredTimers();

    static constexpr std::uint64_t accept_mask_ = std::uint64_t{1} << 63;
    // The timer fd is a singleton, so it gets one reserved id rather than a
    // mask.  It is checked before accept_mask_, and next_id_ starts at 1, so it
    // can never collide with either.
    static constexpr std::uint64_t timer_id_ = std::uint64_t{1} << 62;

    io_uring ring_{};
    std::unordered_map<std::uint64_t, PendingTask> pending_;
    std::uint64_t next_id_ = 1;

    TimerQueue timers_;
    std::deque<Message> ready_;
    std::vector<DelayedOperation*> expired_;
    bool timer_poll_armed_ = false;
};

class IOUringServer final : public Server
{
public:
    IOUringServer(CacheStrategy cache_strategy = CacheStrategy::kHash,
        std::string persistence_directory = "persistent",
        std::pmr::memory_resource* resource = std::pmr::get_default_resource());
    void run(std::uint16_t port, int backlog) override;

protected:
    bool StartReplication(const std::string& address, std::uint16_t port) override;

private:
    struct SessionSlot
    {
        SessionSlot(std::size_t slot_id, std::pmr::memory_resource* resource) :
            id(slot_id), session(resource)
        {
        }

        std::size_t id;
        IOUringSession session;
        std::optional<SessionTask> task;
    };

    SessionTask ServeSession(IOUringSession& session);
    SessionTask ReplicateFromMaster(std::string address, std::uint16_t port);
    void SubmitAccept(SessionSlot& slot);
    void DispatchMessage(const Message& message);
    void Recycle(SessionSlot& slot);

    // Keep the queue alive until all session coroutine frames are destroyed.
    IOUringMessageQueue message_queue_{1024, 2048};
    std::array<std::unique_ptr<SessionSlot>, 1024> sessions_;
    std::optional<SessionTask> replication_task_;
};
} // namespace KV
