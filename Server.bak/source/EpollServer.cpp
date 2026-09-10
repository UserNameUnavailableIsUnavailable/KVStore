#include "Server/EpollServer.hpp"

#include <array>
#include <cerrno>
#include <format>
#include <charconv>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <Foundation/Socket.hpp>

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace KV
{
EpollMessageQueue::EpollMessageQueue()
{
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0)
    {
        throw std::runtime_error(std::format("failed to create epoll instance, errno: {}", errno));
    }
    // The timer heap's single fd is registered once and stays registered: it is
    // level-triggered and always relevant, unlike a per-operation socket.
    ::epoll_event event {.events = EPOLLIN, .data = {.fd = timers_.get_native_handle()}};
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timers_.get_native_handle(), &event) < 0)
    {
        ::close(epoll_fd_);
        throw std::runtime_error(std::format("failed to register timer queue with epoll, errno: {}", errno));
    }
}

EpollMessageQueue::~EpollMessageQueue() noexcept
{
    if (epoll_fd_ >= 0)
    {
        ::close(epoll_fd_);
    }
}

void EpollMessageQueue::RegisterListener(Socket::HandleType handle)
{
    listener_handle_ = handle;
    ::epoll_event event {.events = EPOLLIN, .data = {.fd = handle}};
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, handle, &event) < 0)
    {
        throw std::runtime_error(std::format("failed to register listener with epoll, errno: {}", errno));
    }
}

void EpollMessageQueue::RegisterSession(Socket::HandleType handle)
{
    ::epoll_event event {.events = EPOLLONESHOT | EPOLLRDHUP, .data = {.fd = handle}};
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, handle, &event) < 0)
    {
        throw std::runtime_error(std::format("failed to register client with epoll, errno: {}", errno));
    }
}

void EpollMessageQueue::Unregister(Socket::HandleType handle) noexcept
{
    pending_.erase(handle);
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, handle, nullptr);
}

void EpollMessageQueue::RegisterRead(ReceiveOperation& task)
{
    pending_.insert_or_assign(task.get_handle(), &task);
    if (!Arm(task.get_handle(), EPOLLIN))
    {
        const int error = -errno;
        pending_.erase(task.get_handle());
        task.Complete(error);
        ready_.push_back({.type = MessageType::kRead,
            .session_id = static_cast<std::size_t>(task.get_handle()),
            .result = error,
            .continuation = task.GetContinuation()});
    }
}

void EpollMessageQueue::RegisterWrite(SendOperation& task)
{
    pending_.insert_or_assign(task.get_handle(), &task);
    if (!Arm(task.get_handle(), EPOLLOUT))
    {
        const int error = -errno;
        pending_.erase(task.get_handle());
        task.Complete(error);
        ready_.push_back({.type = MessageType::kWrite,
            .session_id = static_cast<std::size_t>(task.get_handle()),
            .result = error,
            .continuation = task.GetContinuation()});
    }
}

bool EpollMessageQueue::RegisterConnect(ConnectOperation& task)
{
    const Socket::HandleType handle = task.get_handle();
    const int result = ::connect(handle, reinterpret_cast<const ::sockaddr*>(&task.GetAddress()),
        sizeof(task.GetAddress()));
    if (result == 0)
    {
        task.Complete(0);
        return false;
    }
    if (errno != EINPROGRESS && errno != EALREADY)
    {
        task.Complete(-errno);
        return false;
    }
    // This fd is newly created for the replica connection; unlike an accepted
    // client it has not gone through RegisterSession(), so this operation must
    // install the epoll entry with ADD.  Using Arm() here would issue MOD and
    // fail with ENOENT (errno 2), which previously escaped the coroutine.
    ::epoll_event event {.events = EPOLLOUT | EPOLLONESHOT | EPOLLRDHUP,
        .data = {.fd = handle}};
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, handle, &event) < 0)
    {
        task.Complete(-errno);
        return false;
    }
    pending_.insert_or_assign(handle, &task);
    task.SetRegistered(true);
    return true;
}

void EpollMessageQueue::cancelConnect(ConnectOperation& task) noexcept
{
    pending_.erase(task.get_handle());
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, task.get_handle(), nullptr);
}

void EpollMessageQueue::RegisterTimer(DelayedOperation& task)
{
    // No syscall unless this deadline becomes the earliest one outstanding.
    task.SetToken(timers_.Schedule(task.GetDeadline(), &task));
}

void EpollMessageQueue::cancelTimer(DelayedOperation& task) noexcept
{
    timers_.cancel(task.GetToken());
}

void EpollMessageQueue::CollectExpiredTimers()
{
    expired_.clear();
    timers_.DrainExpired(expired_);
    for (DelayedOperation* operation : expired_)
    {
        operation->Complete(true);
        ready_.push_back({.type = MessageType::kTimer,
            .session_id = Message::kNoSession,
            .result = 0,
            .continuation = operation->GetContinuation()});
    }
    expired_.clear();
}

bool EpollMessageQueue::Arm(Socket::HandleType handle, std::uint32_t events) noexcept
{
    ::epoll_event event {.events = events | EPOLLONESHOT | EPOLLRDHUP, .data = {.fd = handle}};
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, handle, &event) == 0)
    {
        return true;
    }

    // A descriptor can legitimately lose its registration between a kernel
    // event and the next coroutine resume (close/EOF, or a stale event from a
    // previous epoll batch).  Retry with ADD for that case; report other
    // failures to the operation instead of throwing through the coroutine.
    if (errno == ENOENT && ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, handle, &event) == 0)
    {
        return true;
    }
    return false;
}

std::optional<Message> EpollMessageQueue::ProcessEvent(const ::epoll_event& event)
{
    const Socket::HandleType handle = event.data.fd;
    if (handle == timers_.get_native_handle())
    {
        CollectExpiredTimers();
        return std::nullopt;
    }

    if (handle == listener_handle_)
    {
        return Message {.type = MessageType::kAccept,
            .session_id = static_cast<std::size_t>(handle),
            .result = 0,
            .continuation = {}};
    }

    auto found = pending_.find(handle);
    if (found == pending_.end())
    {
        return std::nullopt;
    }

    auto& [registered_handle, pending] = *found;
    Message message {.type = MessageType::kRead,
        .session_id = static_cast<std::size_t>(registered_handle),
        .result = 0,
        .continuation = {}};
    int result = std::visit([&message](auto* task) -> int {
        using TaskType = std::remove_pointer_t<decltype(task)>;
        message.continuation = task->GetContinuation();
        if constexpr (std::is_same_v<TaskType, ReceiveOperation>)
        {
            message.type = MessageType::kRead;
            const std::span<char> buffer = task->Get::Foundation::Buffer();
            return static_cast<int>(::recv(task->get_handle(), buffer.data(), buffer.size(), 0));
        }
        else if constexpr (std::is_same_v<TaskType, SendOperation>)
        {
            message.type = MessageType::kWrite;
            const std::span<const char> buffer = task->Get::Foundation::Buffer();
            return static_cast<int>(::send(task->get_handle(), buffer.data(), buffer.size(), MSG_NOSIGNAL));
        }
        else
        {
            message.type = MessageType::kConnect;
            int error = 0;
            socklen_t size = sizeof(error);
            if (::getsockopt(task->get_handle(), SOL_SOCKET, SO_ERROR, &error, &size) < 0)
            {
                return -errno;
            }
            return error == 0 ? 0 : -error;
        }
    }, pending);

    if (result < 0 && message.type != MessageType::kConnect)
    {
        result = -errno;
        if (result == -EAGAIN || result == -EWOULDBLOCK || result == -EINTR)
        {
            if (Arm(registered_handle, message.type == MessageType::kRead ? EPOLLIN : EPOLLOUT))
            {
                return std::nullopt;
            }
            result = -errno;
        }
    }
    std::visit([result](auto* task) { task->Complete(result); }, pending);
    message.result = result;
    pending_.erase(found);
    return message;
}

Message EpollMessageQueue::wait()
{
    while (true)
    {
        if (!ready_.empty())
        {
            const Message message = ready_.front();
            ready_.pop_front();
            return message;
        }

        std::array<::epoll_event, 128> events {};
        const int ready = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
        if (ready < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            throw std::runtime_error(std::format("epoll wait failed, errno: {}", errno));
        }

        // Translate all kernel events before returning the first message. This
        // keeps the syscall batch size at 128 while preserving the existing
        // one-message-at-a-time dispatcher contract.
        for (int index = 0; index < ready; ++index)
        {
            if (const std::optional<Message> message = ProcessEvent(events[index]))
            {
                ready_.push_back(*message);
            }
        }
    }
}

void EpollServer::run(std::uint16_t port, int backlog)
{
    Listen(port, backlog);
	server_socket_.set_non_blocking();
    const Socket::HandleType listener_handle = server_socket_.get_native_handle();
    message_queue_.RegisterListener(listener_handle);

    while (true)
    {
        DispatchMessage(message_queue_.wait());
    }
}

bool EpollServer::StartReplication(const std::string& address, std::uint16_t port)
{
    if (replication_task_.has_value() && !replication_task_->Done())
    {
        return true; // an existing replica connection owns the sync already
    }

    replication_task_.reset();
    master_session_.reset();
    master_session_ = std::make_unique<EpollSession>();
    master_session_->UseMemoryResource(GetMemoryResource());
    replication_task_.emplace(ReplicateFromMaster(address, port));
    return true;
}

SessionTask EpollServer::ReplicateFromMaster(std::string address, std::uint16_t port)
{
    Protocol protocol(GetMemoryResource());

    // Retry connection establishment periodically. Once PSYNC succeeds, this
    // loop is never re-entered: the TCP session remains live until either side
    // closes it. A master EOF therefore stops replication instead of reconnecting
    // behind the user's back.
    while (true)
    {
        master_session_->Reset();
        Socket socket(SocketProtocol::kTcp);
        socket.set_non_blocking();
        master_session_->AttachSocket(std::move(socket));

        if (!(co_await ConnectOperation(message_queue_, *master_session_, address, port)))
        {
            co_await DelayedOperation(message_queue_, std::chrono::seconds(1));
            continue;
        }

        Command request {.name = std::pmr::string("PSYNC", GetMemoryResource()),
            .arguments = std::pmr::vector<std::pmr::string>(GetMemoryResource())};
        request.arguments.emplace_back(master_replication_id_);
        request.arguments.emplace_back(std::to_string(master_replication_offset_));
        master_session_->PrepareRawWrite(protocol.EncodeRequest(request));
        while (!master_session_->GetPendingSend().empty())
        {
            if (co_await SendOperation(message_queue_, *master_session_) == 0)
            {
                co_return;
            }
        }

        std::string wire;
        ResponseDecode decoded;
        while (true)
        {
            const std::span<const std::byte> received =
                co_await ReceiveOperation(message_queue_, *master_session_);
            if (received.empty())
            {
                co_return; // master closed: stop synchronizing, do not reconnect
            }
            wire.append(reinterpret_cast<const char*>(received.data()), received.size());
            master_session_->DiscardRead::Foundation::Buffer();
            decoded = protocol.DecodeResponse(wire);
            if (decoded.status != DecodeStatus::kIncomplete)
            {
                break;
            }
        }

        if (decoded.status != DecodeStatus::kComplete || decoded.result.type != ResultType::kArray ||
            decoded.result.elements.size() != 4 || decoded.result.elements[0].type != ResultType::kSimpleString ||
            decoded.result.elements[3].type != ResultType::kBulkString)
        {
            co_return;
        }

        const bool full_sync = decoded.result.elements[0].value == "FULLRESYNC";
        if (!full_sync && decoded.result.elements[0].value != "CONTINUE")
        {
            co_return;
        }
        std::uint64_t offset = 0;
        const std::string_view text_offset(decoded.result.elements[2].value);
        const auto [end, error] = std::from_chars(text_offset.data(), text_offset.data() + text_offset.size(), offset);
        if (error != std::errc {} || end != text_offset.data() + text_offset.size())
        {
            co_return;
        }

        std::vector<Command> commands;
        const std::string_view snapshot(decoded.result.elements[3].value);
        std::size_t command_offset = 0;
        while (command_offset < snapshot.size())
        {
            RequestDecode command = protocol.DecodeRequest(snapshot.substr(command_offset));
            if (command.status != DecodeStatus::kComplete || command.consumed_bytes == 0)
            {
                co_return;
            }
            command_offset += command.consumed_bytes;
            commands.push_back(std::move(command.command));
        }
        if (full_sync)
        {
            store_ = createStore(cache_strategy_);
        }
        ReplayCommands(commands);
        master_replication_id_ = std::string(decoded.result.elements[1].value);
        master_replication_offset_ = offset;
        wire.erase(0, decoded.consumed_bytes);

        // The master now sends raw replication commands on the same connection.
        // Keep reading until EOF; each complete command advances the offset and
        // is applied without recording it back into the replica's own backlog.
        while (true)
        {
            while (!wire.empty())
            {
                RequestDecode command = protocol.DecodeRequest(wire);
                if (command.status == DecodeStatus::kIncomplete)
                {
                    break;
                }
                if (command.status != DecodeStatus::kComplete || command.consumed_bytes == 0)
                {
                    co_return;
                }
                std::vector<Command> one;
                one.push_back(std::move(command.command));
                ReplayCommands(one);
                ++master_replication_offset_;
                wire.erase(0, command.consumed_bytes);
            }

            const std::span<const std::byte> received =
                co_await ReceiveOperation(message_queue_, *master_session_);
            if (received.empty())
            {
                co_return; // explicit stop condition requested for master EOF
            }
            wire.append(reinterpret_cast<const char*>(received.data()), received.size());
            master_session_->DiscardRead::Foundation::Buffer();
        }
    }
}

SessionTask EpollServer::ServeSession(EpollSession& session)
{
    while (true)
    {
        resolvedRequest request = session.ConsumeRequest();
        if (request.status == RequestStatus::kNeedMore)
        {
            const std::span<const std::byte> received = co_await ReceiveOperation(message_queue_, session);
            if (received.empty())
            {
                co_return;
            }
            continue;
        }

        const Result result = request.status == RequestStatus::kProtocolError
            ? Result {.type = ResultType::kError,
                .value = std::pmr::string(std::format("protocol error: {}", request.error)),
                .elements = std::pmr::vector<Result>()}
            : session.Process(std::move(request.command), [this, &session](const Command& command) {
                return Execute(command, &session);
            });

        session.PrepareResponse(result);
        bool send_complete = false;
        while (!send_complete)
        {
            const std::size_t written = co_await SendOperation(message_queue_, session);
            if (written == 0)
            {
                co_return;
            }
            send_complete = session.GetPendingSend().empty();
        }
    }
}

void EpollServer::DispatchMessage(const Message& message)
{
    if (message.type == MessageType::kAccept)
    {
        AcceptClients();
        return;
    }

    if (message.type == MessageType::kTimer)
    {
        // A timer belongs to no socket, so there is no session to look up here.
        // Route it by continuation and let whoever owns that coroutine decide
        // what happens next; resuming is all this loop has to do.
        if (message.continuation)
        {
            message.continuation.resume();
        }
        return;
    }

    if (master_session_ && replication_task_ &&
        message.session_id == static_cast<std::size_t>(master_session_->GetSocket().get_native_handle()))
    {
        message.continuation.resume();
        if (replication_task_->Done())
        {
            // The connection is over — remove the fd from epoll exactly the way
            // CloseClient does for regular clients, so the two paths stay in
            // sync.  A residual fd registration would look like a live socket
            // to the kernel, and any late event on the same fd would hit
            // pending_.find(), miss, and be silently dropped.
            message_queue_.Unregister(master_session_->GetSocket().get_native_handle());
            replication_task_.reset();
            master_session_.reset();
        }
        return;
    }

    const int fd = static_cast<int>(message.session_id);
    auto found = sessions_.find(fd);
    if (found == sessions_.end())
    {
        return;
    }
    auto& [session_id, entry] = *found;
    message.continuation.resume();
    if (entry.task->Done())
    {
        CloseClient(session_id);
    }
}

void EpollServer::AcceptClients()
{
    while (true)
    {
        const int client_fd = ::accept4(GetSocketHandle(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd >= 0)
        {
            StartClient(client_fd);
            continue;
        }
        if (errno == EINTR) // syscall interrupted by a signal before connection arrives
        {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return;
        }
        throw std::runtime_error(std::format("accept failed, errno: {}", errno));
    }
}

void EpollServer::StartClient(int fd)
{
    message_queue_.RegisterSession(fd);
    auto [iterator, inserted] = sessions_.try_emplace(fd);
    if (!inserted)
    {
        CloseClient(fd);
        return;
    }
    auto& [session_id, entry] = *iterator;
    entry.session.UseMemoryResource(GetMemoryResource());
    entry.session.AttachSocket(session_id);
    entry.task.emplace(ServeSession(entry.session));
    if (entry.task->Done())
    {
        CloseClient(session_id);
    }
}

void EpollServer::CloseClient(Socket::HandleType handle)
{
    message_queue_.Unregister(handle);
    sessions_.erase(handle);
}
} // namespace KV
