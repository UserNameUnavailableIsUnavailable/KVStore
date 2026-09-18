#if defined(__linux__)

#include "URingMultiplexer.hpp"

#include <Foundation/NBIO/Types.hpp>
#include <liburing.h>
#include <poll.h>
#include <sys/socket.h>

#include <Foundation/Core/Socket.hpp>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <stdexcept>
#include <string>
#include <system_error>

#include "Channel.hpp"
#include "AcceptChannel.hpp"
#include "FileStream.hpp"
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
    io_uring_params parameters{
    };
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
        // Every receive armed right now goes in one submission, and the kernel
        // fills the buffers in the order they were queued.
        auto *receive = static_cast<ReceiveChannel *>(channel);
        ::io_uring_prep_recvmsg(sqe, fd, const_cast<msghdr *>(&receive->message_batch()), 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kSend: {
        // One sendmsg for the whole queue: a stream has to keep the order, so the
        // sends cannot be submitted as separate operations.
        auto *send = static_cast<SendChannel *>(channel);
        ::io_uring_prep_sendmsg(sqe, fd, const_cast<msghdr *>(&send->message_batch()), MSG_NOSIGNAL);
        break;
    }
    case Foundation::NBIO::ChannelType::kRead: {
        // Every read armed right now goes in one submission: they cover
        // consecutive stretches of the file, so the kernel takes them as one
        // vector.
        auto *read = static_cast<ReadChannel *>(channel);
        const auto vectors = read->vectors();
        ::io_uring_prep_readv(sqe, fd, vectors.data(), static_cast<unsigned>(vectors.size()),
                              static_cast<__u64>(read->front_offset()));
        break;
    }
    case Foundation::NBIO::ChannelType::kWrite: {
        // Every write armed right now goes in one submission: they follow one
        // another in the file, so the kernel takes them as one vector.
        auto *write = static_cast<WriteChannel *>(channel);
        const auto vectors = write->vectors();
        ::io_uring_prep_writev(sqe, fd, vectors.data(), static_cast<unsigned>(vectors.size()),
                               static_cast<__u64>(write->front_offset()));
        break;
    }
    case Foundation::NBIO::ChannelType::kListen: {
        auto *listen = static_cast<AcceptChannel *>(channel);
        auto *accepted = listen->front();
        if (accepted == nullptr)
        {
            return false;
        }
        // Let the kernel write the peer address straight into the waiting frame's
        // result, which is alive for as long as that frame is parked.
        accepted->address.length() = accepted->address.capacity();
        ::io_uring_prep_accept(sqe, fd, accepted->address.storage<sockaddr>(), &accepted->address.length(), 0);
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
    // The RDMA channels move no data through the ring. They wait for an event on
    // a file descriptor -- a connection-management event, or a work completion
    // on the stream's completion channel -- and then reap it themselves in
    // handle_event(). A one-shot poll on that fd is the entire wait; the
    // channel re-arms it until it has something to report.
    case Foundation::NBIO::ChannelType::kRDMA_Accept:
    case Foundation::NBIO::ChannelType::kRDMA_Connect:
    case Foundation::NBIO::ChannelType::kRDMA_Send:
    case Foundation::NBIO::ChannelType::kRDMA_Receive:
        ::io_uring_prep_poll_add(sqe, fd, POLLIN);
        break;
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
        // The channel turns the byte count into the answers of the receives it
        // covered: one operation can have filled several of them.
        static_cast<ReceiveChannel *>(channel)->complete(result);
        break;
    }
    case Foundation::NBIO::ChannelType::kSend: {
        // As for receiving: one sendmsg can have finished several sends.
        static_cast<SendChannel *>(channel)->complete(result);
        break;
    }
    case Foundation::NBIO::ChannelType::kRead: {
        // The channel turns the byte count into the answers of the reads it
        // covered: one operation can have filled several of them.
        static_cast<ReadChannel *>(channel)->complete(result);
        break;
    }
    case Foundation::NBIO::ChannelType::kWrite: {
        // The channel turns the byte count into the outcomes of the writes it
        // covered: one operation can have finished several of them.
        static_cast<WriteChannel *>(channel)->complete(result);
        break;
    }
    case Foundation::NBIO::ChannelType::kListen: {
        // result is the accepted socket fd, or the reason there is none; the
        // channel turns it into the answer of the wait at the front of its queue.
        static_cast<AcceptChannel *>(channel)->accepted(result);
        break;
    }
    case Foundation::NBIO::ChannelType::kTimer:
        // The read already drained the timerfd; TimerChannel::OnEvent pops the
        // due entries. Nothing to write into a job here.
        break;
    // A poll completion only reports that the fd became readable (its result is
    // a revents mask, not a byte count). The channel reads the event itself, so
    // there is no job to fill here; handle_event() does the rest.
    case Foundation::NBIO::ChannelType::kRDMA_Accept:
    case Foundation::NBIO::ChannelType::kRDMA_Connect:
    case Foundation::NBIO::ChannelType::kRDMA_Send:
    case Foundation::NBIO::ChannelType::kRDMA_Receive:
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
    // Recognised, but only polled: the channel reaps its own events.
    case Foundation::NBIO::ChannelType::kRDMA_Accept:
    case Foundation::NBIO::ChannelType::kRDMA_Connect:
    case Foundation::NBIO::ChannelType::kRDMA_Send:
    case Foundation::NBIO::ChannelType::kRDMA_Receive:
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
#endif // defined(__linux__)
