#include "Server/IOUringServer.hpp"

#include <cerrno>
#include <format>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>
#include "Address.hpp"

#include <poll.h>
#include <sys/socket.h>

namespace KV
{
IOUringMessageQueue::IOUringMessageQueue(std::size_t submission_capacity, std::size_t completion_capacity)
{
    io_uring_params parameters {};
    parameters.flags = IORING_SETUP_CQSIZE;
    parameters.cq_entries = static_cast<unsigned int>(completion_capacity);
    const int result = ::io_uring_queue_init_params(static_cast<unsigned int>(submission_capacity), &ring_, &parameters);
    if (result < 0)
    {
        const int error = -result;
        const std::string reason = std::system_category().message(error);
        if (error == EPERM)
        {
            throw std::runtime_error(std::format(
                "io_uring initialization failed: {} ({}). The kernel supports io_uring, "
                "but this process is not permitted to call io_uring_setup; check the "
                "container seccomp/AppArmor policy or run with an io_uring-enabled security profile",
                error, reason));
        }
        throw std::runtime_error(std::format("io_uring initialization failed: {} ({})", error, reason));
    }
}

IOUringMessageQueue::~IOUringMessageQueue() noexcept
{
    ::io_uring_queue_exit(&ring_);
}

io_uring_sqe& IOUringMessageQueue::GetSubmission()
{
    io_uring_sqe* submission = ::io_uring_get_sqe(&ring_);
    if (submission == nullptr)
    {
        Submit();
        submission = ::io_uring_get_sqe(&ring_);
    }
    if (submission == nullptr)
    {
        throw std::runtime_error("io_uring submission queue is full");
    }
    return *submission;
}

void IOUringMessageQueue::Submit()
{
    const int result = ::io_uring_submit(&ring_);
    if (result < 0)
    {
        throw std::runtime_error(std::format("io_uring submission failed: {}", -result));
    }
}

void IOUringMessageQueue::PrepareAccept(std::size_t session_id, Socket::HandleType listener,
    Address& address)
{
    io_uring_sqe& submission = GetSubmission();
    // Both pointers belong to the Address and outlive this submission, so the
    // kernel can still write through them when the completion arrives.
    ::io_uring_prep_accept(&submission, listener, &address.Storage<::sockaddr>(), &address.GetSize(),
        SOCK_CLOEXEC | SOCK_NONBLOCK);
    ::io_uring_sqe_set_data64(&submission, accept_mask_ | session_id);
    Submit();
}

void IOUringMessageQueue::PrepareRead(std::uint64_t id, ReceiveOperation& task)
{
    io_uring_sqe& submission = GetSubmission();
    const std::span<char> buffer = task.Get::Foundation::Buffer();
    ::io_uring_prep_recv(&submission, task.get_handle(), buffer.data(), buffer.size(), 0);
    ::io_uring_sqe_set_data64(&submission, id);
    Submit();
}

void IOUringMessageQueue::PrepareWrite(std::uint64_t id, SendOperation& task)
{
    io_uring_sqe& submission = GetSubmission();
    const std::span<const char> buffer = task.Get::Foundation::Buffer();
    ::io_uring_prep_send(&submission, task.get_handle(), buffer.data(), buffer.size(), MSG_NOSIGNAL);
    ::io_uring_sqe_set_data64(&submission, id);
    Submit();
}

// The timer fd is waited on with poll rather than read on purpose.  A read SQE
// needs a destination buffer, and a late completion would write into it after
// the queue may have moved on; poll touches no user memory, so the queue can
// drain the fd itself, synchronously, in wait().
void IOUringMessageQueue::PollTimerQueue()
{
    if (timer_poll_armed_)
    {
        return;
    }
    io_uring_sqe& submission = GetSubmission();
    ::io_uring_prep_poll_add(&submission, timers_.get_native_handle(), POLLIN);
    ::io_uring_sqe_set_data64(&submission, timer_id_);
    Submit();
    timer_poll_armed_ = true;
}

void IOUringMessageQueue::CollectExpiredTimers()
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

void IOUringMessageQueue::PrepareConnect(std::uint64_t id, ConnectOperation& task)
{
    io_uring_sqe& submission = GetSubmission();
    ::io_uring_prep_connect(&submission, task.get_handle(),
        reinterpret_cast<const ::sockaddr*>(&task.GetAddress()), sizeof(task.GetAddress()));
    ::io_uring_sqe_set_data64(&submission, id);
    Submit();
}

bool IOUringMessageQueue::RegisterConnect(ConnectOperation& task)
{
    const std::uint64_t id = next_id_++;
    pending_.emplace(id, &task);
    PrepareConnect(id, task);
    task.SetRegistered(true);
    return true;
}

void IOUringMessageQueue::cancelConnect(ConnectOperation& task) noexcept
{
    // The completion is ignored if it arrives after the coroutine is cancelled.
    // The current operation table is keyed by the generated id, so this needs a
    // reverse lookup until the operation registry is introduced.
    for (auto it = pending_.begin(); it != pending_.end(); ++it)
    {
        if (std::holds_alternative<ConnectOperation*>(it->second) &&
            std::get<ConnectOperation*>(it->second) == &task)
        {
            pending_.erase(it);
            return;
        }
    }
}

void IOUringMessageQueue::RegisterRead(ReceiveOperation& task)
{
    const std::uint64_t id = next_id_++;
    pending_.emplace(id, &task);
    PrepareRead(id, task);
}

void IOUringMessageQueue::RegisterWrite(SendOperation& task)
{
    const std::uint64_t id = next_id_++;
    pending_.emplace(id, &task);
    PrepareWrite(id, task);
}

void IOUringMessageQueue::RegisterTimer(DelayedOperation& task)
{
    task.SetToken(timers_.Schedule(task.GetDeadline(), &task));
    PollTimerQueue();
}

void IOUringMessageQueue::cancelTimer(DelayedOperation& task) noexcept
{
    timers_.cancel(task.GetToken());
}

Message IOUringMessageQueue::wait()
{
    while (true)
    {
        // One expiration can release a whole batch of coroutines, so serve
        // anything already collected before going back into the kernel.
        if (!ready_.empty())
        {
            const Message message = ready_.front();
            ready_.pop_front();
            return message;
        }

        io_uring_cqe* completion = nullptr;
        const int wait_result = ::io_uring_wait_cqe(&ring_, &completion);
        if (wait_result == -EINTR)
        {
            continue;
        }
        if (wait_result < 0)
        {
            throw std::runtime_error(std::format("io_uring completion wait failed: {}", -wait_result));
        }

        const std::uint64_t id = ::io_uring_cqe_get_data64(completion);
        const int result = completion->res;
        ::io_uring_cqe_seen(&ring_, completion);

        if (id == timer_id_)
        {
            timer_poll_armed_ = false;
            CollectExpiredTimers();
            // Keep watching while any deadline is still outstanding.
            if (!timers_.Empty())
            {
                PollTimerQueue();
            }
            continue;
        }

        if ((id & accept_mask_) != 0)
        {
            return {.type = MessageType::kAccept,
                .session_id = static_cast<std::size_t>(id & ~accept_mask_),
                .result = result,
                .continuation = {}};
        }

        auto found = pending_.find(id);
        if (found == pending_.end())
        {
            continue;
        }
        auto& [operation_id, pending] = *found;
        if (result == -EAGAIN || result == -EINTR)
        {
            std::visit([this, operation_id](auto* task) {
                using TaskType = std::remove_pointer_t<decltype(task)>;
                if constexpr (std::is_same_v<TaskType, ReceiveOperation>)
                {
                    PrepareRead(operation_id, *task);
                }
                else if constexpr (std::is_same_v<TaskType, SendOperation>)
                {
                    PrepareWrite(operation_id, *task);
                }
                else
                {
                    PrepareConnect(operation_id, *task);
                }
            }, pending);
            continue;
        }

        Message message {.type = MessageType::kRead,
            .session_id = 0,
            .result = result,
            .continuation = {}};
        std::visit([result, &message](auto* task) {
            using TaskType = std::remove_pointer_t<decltype(task)>;
            if constexpr (std::is_same_v<TaskType, ConnectOperation>)
            {
                message.type = MessageType::kConnect;
            }
            else
            {
                message.type = std::is_same_v<TaskType, ReceiveOperation>
                    ? MessageType::kRead
                    : MessageType::kWrite;
            }
            message.session_id = static_cast<std::size_t>(task->get_handle());
            message.continuation = task->GetContinuation();
            task->Complete(result);
        }, pending);
        pending_.erase(found);
        return message;
    }
}

IOUringServer::IOUringServer(CacheStrategy cache_strategy,
    std::string persistence_directory, std::pmr::memory_resource* resource) :
    Server(cache_strategy, std::move(persistence_directory), resource)
{
    for (std::size_t index = 0; index < sessions_.size(); ++index)
    {
        sessions_[index] = std::make_unique<SessionSlot>(index, resource);
    }
}

SessionTask IOUringServer::ServeSession(IOUringSession& session)
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
        while (!session.GetPendingSend().empty())
        {
            if (co_await SendOperation(message_queue_, session) == 0)
            {
                co_return;
            }
        }
    }
}

bool IOUringServer::StartReplication(const std::string& address, std::uint16_t port)
{
    if (replication_task_.has_value() && !replication_task_->Done())
    {
        return true;
    }
    replication_task_.reset();
    master_session_.reset();
    master_session_ = std::make_unique<IOUringSession>(GetMemoryResource());
    replication_task_.emplace(ReplicateFromMaster(address, port));
    return true;
}

SessionTask IOUringServer::ReplicateFromMaster(std::string address, std::uint16_t port)
{
    Protocol protocol(GetMemoryResource());
    while (true)
    {
        master_session_->Reset();
        Socket socket(SocketProtocol::kTcp);
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
                co_return;
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
                co_return;
            }
            wire.append(reinterpret_cast<const char*>(received.data()), received.size());
            master_session_->DiscardRead::Foundation::Buffer();
        }
    }
}

void IOUringServer::SubmitAccept(SessionSlot& slot)
{
    slot.session.WithMutableAcceptContext([this, &slot](Address& address) {
        message_queue_.PrepareAccept(slot.id, GetSocketHandle(), address);
    });
}

void IOUringServer::DispatchMessage(const Message& message)
{
    if (message.type == MessageType::kAccept)
    {
        SessionSlot& slot = *sessions_.at(message.session_id);
        if (message.result < 0)
        {
            SubmitAccept(slot);
            return;
        }
        // The kernel already wrote the peer address and its length straight
        // into the session's Address, so there is nothing to fix up here.
        slot.session.AttachSocket(message.result);
        slot.task.emplace(ServeSession(slot.session));
        if (slot.task->Done())
        {
            Recycle(slot);
        }
        return;
    }

    if (message.type == MessageType::kTimer)
    {
        // A timer belongs to no session, so there is nothing to look up and
        // nothing to recycle -- resuming the continuation is the whole job.
        if (message.continuation)
        {
            message.continuation.resume();
        }
        return;
    }

    if (master_session_ && replication_task_ &&
        message.session_id == static_cast<std::size_t>(master_session_->GetSocket().get_native_handle()))
    {
        if (message.continuation)
        {
            message.continuation.resume();
        }
        if (replication_task_->Done())
        {
            replication_task_.reset();
            master_session_.reset();
        }
        return;
    }

    const auto found = std::ranges::find_if(sessions_, [&message](const auto& slot) {
        return slot->session.GetSocket().get_native_handle() == static_cast<Socket::HandleType>(message.session_id);
    });
    if (found == sessions_.end())
    {
        return;
    }
    SessionSlot& slot = **found;
    message.continuation.resume();
    if (slot.task->Done())
    {
        Recycle(slot);
    }
}

void IOUringServer::Recycle(SessionSlot& slot)
{
    slot.task.reset();
    slot.session.Reset();
    SubmitAccept(slot);
}

void IOUringServer::run(std::uint16_t port, int backlog)
{
    Listen(port, backlog);
    for (const auto& slot : sessions_)
    {
        SubmitAccept(*slot);
    }
    while (true)
    {
        DispatchMessage(message_queue_.wait());
    }
}
} // namespace KV
