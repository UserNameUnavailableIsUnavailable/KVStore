#include "URingMultiplexer.hpp"

#include <Foundation/NBIO/Types.hpp>
#include <liburing.h>
#include <sys/socket.h>

#include <Foundation/Core/Socket.hpp>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <stdexcept>
#include <string>
#include <system_error>

#include <Foundation/NBIO/Channel.hpp>
#include "ListenChannel.hpp"
#include "ReadChannel.hpp"
#include "NotifyChannel.hpp"
#include "ReceiveChannel.hpp"
#include "SendChannel.hpp"
#include "SignalChannel.hpp"
#include "WriteChannel.hpp"
#include "TimerChannel.hpp"

#define MAKE_ERROR_CODE(e) std::error_code(e, std::system_category())

namespace Foundation::NBIO
{
#define ThrowUringError(error, what)                                                                                   \
    throw std::system_error(error, std::system_category(),                                                             \
                            std::string(what) + ": " + std::system_category().message(error));

URingMultiplexer::URingMultiplexer(std::uint32_t submission_capacity, std::uint32_t completion_capacity) :
	Multiplexer(MultiplexerType::kURing)
{
    // Zero-initialised: only the fields we set may influence setup.
    io_uring_params parameters{};
    parameters.flags = IORING_SETUP_CQSIZE;
    parameters.cq_entries = completion_capacity;

    const int ret = ::io_uring_queue_init_params(submission_capacity, &ring_, &parameters);
    if (ret < 0)
    {
        const int error = -ret;
        if (error == EPERM)
        {
            throw std::runtime_error("io_uring initialization failed: " + std::system_category().message(error) +
                                     ". The kernel supports io_uring, but this process is not permitted to call "
                                     "io_uring_setup; check the container seccomp/AppArmor policy or run with an "
                                     "io_uring-enabled security profile.\n"
                                     "If you are using Docker, you may try restart docker with `--security-opt "
                                     "seccomp=unconfined`.");
        }
        ThrowUringError(error, "io_uring_queue_init_params failed");
    }
}

URingMultiplexer::~URingMultiplexer() noexcept
{
    ::io_uring_queue_exit(&ring_);
}

bool URingMultiplexer::prepare(Foundation::NBIO::Channel *channel)
{
    io_uring_sqe *sqe = ::io_uring_get_sqe(&ring_);
    if (sqe == nullptr)
    {
        // Submission queue is full: hand the entries prepared so far to the
        // kernel to make room, then try once more.
        ::io_uring_submit(&ring_);
        sqe = ::io_uring_get_sqe(&ring_);
        if (sqe == nullptr)
        {
            return false; // still full: the caller retries on a later iteration
        }
    }

    const int fd = channel->native_handle();
    switch (channel->type())
    {
    case Foundation::NBIO::ChannelType::kReceive: {
        auto *receive = static_cast<ReceiveChannel *>(channel);
        auto &job = receive->job();
        ::io_uring_prep_recv(sqe, fd, job.buffer->appendable_span().data(), job.buffer->appendable_size(), 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kSend: {
        auto *send = static_cast<SendChannel *>(channel);
        auto &job = send->job();
        ::io_uring_prep_send(sqe, fd, job.buffer->valid_span().data(), job.buffer->valid_size(), MSG_NOSIGNAL);
        break;
    }
    case Foundation::NBIO::ChannelType::kRead: {
        auto *read = static_cast<ReadChannel *>(channel);
        auto &job = read->job();
        ::io_uring_prep_read(sqe, fd, job.buffer->appendable_span().data(), job.buffer->appendable_size(), job.offset);
        break;
    }
    case Foundation::NBIO::ChannelType::kWrite: {
        auto *write = static_cast<WriteChannel *>(channel);
        auto &job = write->job();
        ::io_uring_prep_write(sqe, fd, job.buffer->valid_span().data(), job.buffer->valid_size(), job.offset);
        break;
    }
    case Foundation::NBIO::ChannelType::kListen: {
        auto *listen = static_cast<ListenChannel *>(channel);
        auto &job = listen->job();
        // Let the kernel write the peer address straight into the job's result.
        job.result.address.length() = job.result.address.capacity();
        ::io_uring_prep_accept(sqe, fd, job.result.address.storage<sockaddr>(), &job.result.address.length(), 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kTimer: {
        // A timerfd read completes when the timer fires, returning (and
        // draining) the 8-byte expiration count -- so a single read both waits
        // and consumes the event, matching the completion model.
        auto &timer_expirations = static_cast<TimerChannel *>(channel)->last_expirations();
        ::io_uring_prep_read(sqe, fd, &timer_expirations, sizeof(timer_expirations), 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kNotify: {
        auto &count = static_cast<NotifyChannel *>(channel)->count();
        ::io_uring_prep_read(sqe, fd, &count, sizeof(count), 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kSignal: {
        auto &count = static_cast<SignalChannel *>(channel)->count();
        ::io_uring_prep_read(sqe, fd, &count, sizeof(count), 0);
        break;
    }
    default:
        return false;
    }

    // The channel identifies itself; its type says which operation completed.
    ::io_uring_sqe_set_data(sqe, channel);
    return true;
}

void URingMultiplexer::submit()
{
    if (pending_submissions_.empty())
    {
        return;
    }

    std::size_t prepared = 0;
    auto it = pending_submissions_.begin();
    while (it != pending_submissions_.end())
    {
        if (!prepare(*it))
        {
            // Queue exhausted: keep the remainder (FIFO) for the next iteration.
            break;
        }
        it = pending_submissions_.erase(it);
        ++prepared;
    }

    if (prepared > 0)
    {
        const int ret = ::io_uring_submit(&ring_);
        if (ret < 0)
        {
            ThrowUringError(-ret, "io_uring_submit failed");
        }
    }
}

void URingMultiplexer::complete(Foundation::NBIO::Channel *channel, int result)
{
    switch (channel->type())
    {
    case Foundation::NBIO::ChannelType::kReceive: {
        auto &job = static_cast<ReceiveChannel *>(channel)->job();
        if (result > 0)
        {
            job.buffer->commit(static_cast<std::size_t>(result));
            job.result.bytes_transferred = static_cast<std::size_t>(result);
            job.result.status = ::Foundation::Core::ReceiveStatus::kDone;
        }
        else if (result == 0)
        {
            job.result.status = ::Foundation::Core::ReceiveStatus::kPeerClosed;
        }
        else if (result == -EAGAIN || result == -EWOULDBLOCK)
        {
            job.result.status = ::Foundation::Core::ReceiveStatus::kPending; // retry later
        }
        else
        {
            job.result.status = ::Foundation::Core::ReceiveStatus::kError;
            job.result.error_code = Foundation::Core::Socket::get_last_error();
        }
        break;
    }
    case Foundation::NBIO::ChannelType::kSend: {
        auto &job = static_cast<SendChannel *>(channel)->job();
        if (result >= 0)
        {
            job.buffer->consume(static_cast<std::size_t>(result));
            job.result.bytes_transferred += static_cast<std::size_t>(result);
            // "write flushes all": done only when nothing is left to send.
            job.result.status = (job.buffer->valid_size() == 0) ? ::Foundation::Core::SendStatus::kDone : ::Foundation::Core::SendStatus::kPending;
        }
        else if (result == -EAGAIN || result == -EWOULDBLOCK)
        {
            job.result.status = ::Foundation::Core::SendStatus::kPending;
        }
        else
        {
            job.result.status = ::Foundation::Core::SendStatus::kError;
            job.result.error_code = MAKE_ERROR_CODE(static_cast<unsigned int>(-result));
        }
        break;
    }
    case Foundation::NBIO::ChannelType::kRead: {
        auto &job = static_cast<ReadChannel *>(channel)->job();
        if (result > 0)
        {
            job.buffer->commit(static_cast<std::size_t>(result));
            job.offset += static_cast<std::uint64_t>(result);
            job.result.bytes_transferred = static_cast<std::size_t>(result);
            job.result.status = Foundation::Core::ReadStatus::kDone;
        }
        else if (result == 0)
        {
            job.result.status = Foundation::Core::ReadStatus::kEndOfFile;
        }
        else if (result == -EAGAIN || result == -EWOULDBLOCK)
        {
            job.result.status = Foundation::Core::ReadStatus::kPending;
        }
        else
        {
            job.result.status = Foundation::Core::ReadStatus::kError;
            job.result.error_code = MAKE_ERROR_CODE(static_cast<unsigned int>(-result));
        }
        break;
    }
    case Foundation::NBIO::ChannelType::kWrite: {
        auto &job = static_cast<WriteChannel *>(channel)->job();
        if (result >= 0)
        {
            job.buffer->consume(static_cast<std::size_t>(result));
            job.offset += static_cast<std::uint64_t>(result);
            job.result.bytes_transferred += static_cast<std::size_t>(result);
            job.result.status = (job.buffer->valid_size() == 0) ? Foundation::Core::WriteStatus::kDone : Foundation::Core::WriteStatus::kPending;
        }
        else if (result == -EAGAIN || result == -EWOULDBLOCK)
        {
            job.result.status = Foundation::Core::WriteStatus::kPending;
        }
        else
        {
            job.result.status = Foundation::Core::WriteStatus::kError;
            job.result.error_code = MAKE_ERROR_CODE(static_cast<unsigned int>(-result));
        }
        break;
    }
    case Foundation::NBIO::ChannelType::kListen: {
        auto &job = static_cast<ListenChannel *>(channel)->job();
        if (result >= 0)
        {
            // result is the accepted socket fd; the peer address was filled in
            // place by the kernel when the operation was submitted.
            job.result.status = ::Foundation::Core::AcceptStatus::kDone;
            job.result.socket = Foundation::Core::Socket::Adopt(result);
        }
        else if (result == -EAGAIN || result == -EWOULDBLOCK)
        {
            job.result.status = ::Foundation::Core::AcceptStatus::kPending;
        }
        else
        {
            job.result.status = ::Foundation::Core::AcceptStatus::kError;
            job.result.error_code = MAKE_ERROR_CODE(static_cast<unsigned int>(-result));
        }
        break;
    }
    case Foundation::NBIO::ChannelType::kTimer:
        // The read already drained the timerfd; TimerChannel::OnEvent pops the
        // due entries. Nothing to write into a job here.
        break;
    default:
        break;
    }
}

void URingMultiplexer::handle_completions()
{
    io_uring_cqe *cqe = nullptr;
    while (::io_uring_peek_cqe(&ring_, &cqe) == 0 && cqe != nullptr)
    {
        auto *channel = static_cast<Foundation::NBIO::Channel *>(::io_uring_cqe_get_data(cqe));
        if (channel != nullptr)
        {
            // Fill the job from the completion, then let the channel do job
            // control (resume, or stay armed for an unfinished operation).
            complete(channel, cqe->res);
            channel->disarm();
            channel->handle_event();
        }
        ::io_uring_cqe_seen(&ring_, cqe);
    }
}

void URingMultiplexer::run_impl(int timeout_ms)
{
    // 1. Give the kernel every operation that is waiting to start.
    submit();

    // 2. wait for at least one completion (unless asked not to block).
    io_uring_cqe *cqe = nullptr;
    int ret = 0;
    if (timeout_ms < 0)
    {
        do
        {
            ret = ::io_uring_wait_cqe(&ring_, &cqe);
        } while (ret == -EINTR || ret == EINTR);
    }
    else
    {
        __kernel_timespec ts{.tv_sec = static_cast<long long>(timeout_ms / 1000),
                             .tv_nsec = static_cast<long long>(timeout_ms % 1000) * 1000000LL};
        do
        {
            ret = ::io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
        } while (ret == -EINTR || ret == EINTR);
    }
    // -ETIME simply means "nothing completed" for the timeout variant.
    if (ret < 0 && ret != -ETIME)
    {
        ThrowUringError(-ret, "io_uring_wait_cqe failed");
    }

    // 3. Reap everything that is ready. A resumed coroutine may arm the next
    //    operation, so submit again to keep latency down.
    handle_completions();
    submit();
}

void URingMultiplexer::run()
{
    run_impl(-1);
}

void URingMultiplexer::run_for(std::chrono::milliseconds timeout)
{
    auto now = std::chrono::steady_clock::now();
    const auto due = now + timeout;
    do
    {
        const auto remaining = (due - now).count();
        const auto ms = remaining > INT_MAX ? INT_MAX : remaining;
        run_impl(static_cast<int>(ms));
        now = std::chrono::steady_clock::now();
    } while (now < due);
}

void URingMultiplexer::add_channel(Foundation::NBIO::Channel *channel)
{
    const auto fd = channel->native_handle();

    // No IOHandler is installed: this backend performs no data movement in a
    // callback. It only needs to recognise the channel type.
    switch (channel->type())
    {
    case Foundation::NBIO::ChannelType::kReceive:
    case Foundation::NBIO::ChannelType::kSend:
    case Foundation::NBIO::ChannelType::kRead:
    case Foundation::NBIO::ChannelType::kWrite:
    case Foundation::NBIO::ChannelType::kListen:
    case Foundation::NBIO::ChannelType::kTimer:
    case Foundation::NBIO::ChannelType::kNotify:
        break;
    default:
        throw std::logic_error("URingMultiplexer: unsupported channel type");
    }

    registered_channels_.emplace(fd, channel);
}

void URingMultiplexer::update_channel(Foundation::NBIO::Channel *channel)
{
    // Called by Channel::Arm()/Disarm(). "Armed" means an operation is pending,
    // so it needs a submission queue entry; disarmed means drop any queued one.
    if (channel->armed())
    {
        if (std::find(pending_submissions_.begin(), pending_submissions_.end(), channel) == pending_submissions_.end())
        {
            pending_submissions_.push_back(channel);
        }
        return;
    }
    std::erase(pending_submissions_, channel);
}

void URingMultiplexer::delete_channel(Foundation::NBIO::Channel *channel) noexcept
{
    const auto fd = channel->native_handle();
    auto range = registered_channels_.equal_range(fd);
    for (auto it = range.first; it != range.second; ++it)
    {
        if (it->second == channel)
        {
            registered_channels_.erase(it);
            break;
        }
    }
    // Drop any submission that has not reached the kernel yet.
    std::erase(pending_submissions_, channel);
    // TODO: cancel operations already in flight (IORING_OP_ASYNC_cancel) and
    // wait for the cancellation to complete before the channel's buffers go
    // away. Today channels outlive their in-flight operations because a session
    // is only reclaimed after its coroutine finishes.
}
} // namespace Foundation::NBIO
