#pragma once

#if not defined (__linux__)
#error "This header is linux-specific."
#endif

#include "EpollSession.hpp"
#include "Message.hpp"
#include "Task.hpp"
#include "TimerQueue.hpp"

#include <cstdint>
#include <deque>
#include <sys/epoll.h>
#include <optional>
#include <unordered_map>
#include <variant>
#include <vector>

#include "Server/Server.hpp"


namespace KV
{
class EpollMessageQueue final : public MessageQueue
{
public:
    EpollMessageQueue();
    ~EpollMessageQueue() noexcept override;

    void RegisterListener(Socket::HandleType handle);
    void RegisterSession(Socket::HandleType handle);
    void Unregister(Socket::HandleType handle) noexcept;
    void RegisterRead(ReceiveOperation& task) override;
    void RegisterWrite(SendOperation& task) override;
    bool RegisterConnect(ConnectOperation& task) override;
    void cancelConnect(ConnectOperation& task) noexcept override;
    void RegisterTimer(DelayedOperation& task) override;
    void cancelTimer(DelayedOperation& task) noexcept override;
    Message wait() override;

private:
    using PendingTask = std::variant<ReceiveOperation*, SendOperation*, ConnectOperation*>;

    bool Arm(Socket::HandleType handle, std::uint32_t events) noexcept;
    // Translate one kernel event into a logical message. A null result means the
    // event was stale, EAGAIN, or a timer event that only populated ready_.
    std::optional<Message> ProcessEvent(const ::epoll_event& event);
    // Move every expired timer's continuation into ready_.
    void CollectExpiredTimers();

    int epoll_fd_ = -1;
    Socket::HandleType listener_handle_ = -1;
    std::unordered_map<Socket::HandleType, PendingTask> pending_;

    // One timerfd backs every pending delay; the heap decides which deadline it
    // is currently armed for.
    TimerQueue timers_;

    // A single expiration can release several coroutines, but wait() hands back
    // one message at a time, so the surplus waits here.
    std::deque<Message> ready_;
    std::vector<DelayedOperation*> expired_;
};

class EpollServer final : public Server
{
public:
    using Server::Server;

    void run(std::uint16_t, int backlog) override;

protected:
    bool StartReplication(const std::string& address, std::uint16_t port) override;

private:
    struct SessionEntry
    {
        EpollSession session;
        std::optional<SessionTask> task;
    };

    SessionTask ServeSession(EpollSession& session);
    SessionTask ReplicateFromMaster(std::string address, std::uint16_t port);
    void AcceptClients();
    void DispatchMessage(const Message& message);
    void StartClient(Socket::HandleType handle);
    void CloseClient(Socket::HandleType handle);

    // The queue must outlive every coroutine frame that may unregister a
    // pending operation from its destructor.
    EpollMessageQueue message_queue_;
    std::unordered_map<int, SessionEntry> sessions_;
    std::optional<SessionTask> replication_task_;
};
} // namespace KV
