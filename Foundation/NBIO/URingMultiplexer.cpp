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
#include "RDMA_AcceptChannel.hpp"
#include "RDMA_ConnectChannel.hpp"
#include "RDMA_ReceiveChannel.hpp"
#include "RDMA_SendChannel.hpp"
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

// The channels that carry per-operation waits. They are queued for a submission
// only while they have work the kernel has not been given yet; the others (a
// timer, a notifier, a signal, an RDMA stream) are queued whenever they are armed.
static bool waits_per_operation(ChannelType type)
{
    switch (type)
    {
    case ChannelType::kReceive:
    case ChannelType::kSend:
    case ChannelType::kRead:
    case ChannelType::kWrite:
    case ChannelType::kAccept:
        return true;
    default:
        return false;
    }
}

// Does this channel have work the kernel has not been given yet? Only the channels
// that carry per-operation waits are asked: the others are queued whenever they are
// armed.
static bool has_prepared(Foundation::NBIO::Channel *channel)
{
    switch (channel->type())
    {
    case ChannelType::kReceive:
        return static_cast<ReceiveChannel *>(channel)->has_prepared();
    case ChannelType::kSend:
        return static_cast<SendChannel *>(channel)->has_prepared();
    case ChannelType::kRead:
        return static_cast<ReadChannel *>(channel)->has_prepared();
    case ChannelType::kWrite:
        return static_cast<WriteChannel *>(channel)->has_prepared();
    case ChannelType::kAccept:
        return static_cast<AcceptChannel *>(channel)->has_prepared();
    default:
        return false;
    }
}

// Wakes what a completion answered and lets the channel say what it owes next.
// Every channel is driven through here, which is why the multiplexer never has to
// know which of them keep a queue of waits.
static void resume_channel(Foundation::NBIO::Channel *channel)
{
    switch (channel->type())
    {
    case ChannelType::kReceive:
        static_cast<ReceiveChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kSend:
        static_cast<SendChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kRead:
        static_cast<ReadChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kWrite:
        static_cast<WriteChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kAccept:
        static_cast<AcceptChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kTimer:
        static_cast<TimerChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kNotify:
        static_cast<NotifyChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kSignal:
        static_cast<SignalChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kRDMA_Accept:
        static_cast<RDMA_AcceptChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kRDMA_Connect:
        static_cast<RDMA_ConnectChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kRDMA_Send:
        static_cast<RDMA_SendChannel *>(channel)->handle_completion();
        break;
    case ChannelType::kRDMA_Receive:
        static_cast<RDMA_ReceiveChannel *>(channel)->handle_completion();
        break;
    default:
        break;
    }
}

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
    if (waits_per_operation(channel->type()) && !has_prepared(channel))
    {
        // Nothing the kernel can take. A channel that carries per-operation waits
        // arms itself the moment it has some, so this is the normal state for one
        // whose operation is already in flight.
        return false;
    }

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

    switch (channel->type())
    {
    case Foundation::NBIO::ChannelType::kReceive: {
        // Every prepared receive goes in one submission, and the kernel fills the
        // buffers in the order they were queued.
        auto *receive = static_cast<ReceiveChannel *>(channel);
        if (receive->count_prepared() == 0)
        {
            return false;
        }
        ::io_uring_prep_recvmsg(sqe, receive->native_handle(), const_cast<msghdr *>(&receive->message_batch()), 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kSend: {
        // One sendmsg for the whole queue: a stream has to keep the order, so the
        // sends cannot be submitted as separate operations.
        auto *send = static_cast<SendChannel *>(channel);
        if (send->count_prepared() == 0)
        {
            return false;
        }
        ::io_uring_prep_sendmsg(sqe, send->native_handle(), const_cast<msghdr *>(&send->message_batch()), MSG_NOSIGNAL);
        break;
    }
    case Foundation::NBIO::ChannelType::kRead: {
        // Every prepared read goes in one submission: they cover consecutive
        // stretches of the file, so the kernel takes them as one vector.
        auto *read = static_cast<ReadChannel *>(channel);
        if (read->count_prepared() == 0)
        {
            return false;
        }
        const auto vectors = read->vectors();
        ::io_uring_prep_readv(sqe, read->native_handle(), vectors.data(), static_cast<unsigned>(vectors.size()),
                              static_cast<__u64>(read->front_offset()));
        break;
    }
    case Foundation::NBIO::ChannelType::kWrite: {
        // Every prepared write goes in one submission: they follow one another in
        // the file, so the kernel takes them as one vector.
        auto *write = static_cast<WriteChannel *>(channel);
        if (write->count_prepared() == 0)
        {
            return false;
        }
        const auto vectors = write->vectors();
        ::io_uring_prep_writev(sqe, write->native_handle(), vectors.data(), static_cast<unsigned>(vectors.size()),
                               static_cast<__u64>(write->front_offset()));
        break;
    }
    case Foundation::NBIO::ChannelType::kAccept: {
        // One accept covers one wait: the answer names the channel, not the wait
        // it belongs to, so exactly one may be with the kernel at a time.
        auto *accept = static_cast<AcceptChannel *>(channel);
        if (accept->count_prepared() == 0)
        {
            return false;
        }

        PendingAccept *waiting = accept->submitted_front();
        // Let the kernel write the peer address straight into the waiting frame's
        // result, which is alive for as long as that frame is parked.
        waiting->result().address.length() = waiting->result().address.capacity();
        ::io_uring_prep_accept(sqe, accept->native_handle(), waiting->result().address.storage<sockaddr>(),
                               &waiting->result().address.length(), 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kTimer: {
        // A timerfd read completes when the timer fires, returning (and
        // draining) the 8-byte expiration count -- so a single read both waits
        // and consumes the event, matching the completion model.
        auto timer_channel = static_cast<TimerChannel *>(channel);
        auto &timer_expirations = timer_channel->last_expirations();
        ::io_uring_prep_read(sqe, timer_channel->native_handle(), &timer_expirations, sizeof(timer_expirations), 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kNotify: {
        auto notify_channel = static_cast<NotifyChannel*>(channel);
        auto &count = static_cast<NotifyChannel *>(channel)->count();
        ::io_uring_prep_read(sqe, notify_channel->native_handle(), &count, sizeof(count), 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kSignal: {
        auto signal_channel = static_cast<SignalChannel*>(channel);
        auto &count = static_cast<SignalChannel *>(channel)->count();
        ::io_uring_prep_read(sqe, signal_channel->native_handle(), &count, sizeof(count), 0);
        break;
    }
    // The RDMA channels move no data through the ring. They wait for an event on
    // a file descriptor -- a connection-management event, or a work completion
    // on the stream's completion channel -- and then reap it themselves in
    // handle_completion(). A one-shot poll on that fd is the entire wait; the
    // channel re-arms it until it has something to report.
    case Foundation::NBIO::ChannelType::kRDMA_Accept:
    case Foundation::NBIO::ChannelType::kRDMA_Connect:
    case Foundation::NBIO::ChannelType::kRDMA_Send:
    case Foundation::NBIO::ChannelType::kRDMA_Receive:
		::io_uring_prep_poll_add(sqe, channel->native_handle(), POLLIN);
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

    std::size_t handed_over = 0;
    std::vector<Foundation::NBIO::Channel *> still_waiting;
    still_waiting.reserve(pending_submissions_.size());

    for (Foundation::NBIO::Channel *channel : pending_submissions_)
    {
        if (waits_per_operation(channel->type()) && !has_prepared(channel))
        {
            // Nothing left to hand over: the channel arms itself when there is,
            // so it leaves the queue until then.
            continue;
        }

        if (!prepare(channel))
        {
            // The submission queue is exhausted: this channel and everything
            // behind it keep their places, and their order, for the next pass.
            still_waiting.push_back(channel);
            continue;
        }

        ++handed_over;

        // A channel may have more than one operation to hand over -- an accept
        // covers one wait at a time -- so it stays queued while it has more.
        if (has_prepared(channel))
        {
            still_waiting.push_back(channel);
        }
    }

    pending_submissions_.swap(still_waiting);

    if (handed_over > 0)
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
    case Foundation::NBIO::ChannelType::kReceive:
        static_cast<ReceiveChannel *>(channel)->complete_tasks(result);
        break;
    case Foundation::NBIO::ChannelType::kSend:
        static_cast<SendChannel *>(channel)->complete_tasks(result);
        break;
    case Foundation::NBIO::ChannelType::kRead:
        static_cast<ReadChannel *>(channel)->complete_tasks(result);
        break;
    case Foundation::NBIO::ChannelType::kWrite:
        static_cast<WriteChannel *>(channel)->complete_tasks(result);
        break;
    case Foundation::NBIO::ChannelType::kAccept:
        static_cast<AcceptChannel *>(channel)->complete_tasks(result);
        break;
    case Foundation::NBIO::ChannelType::kTimer:
        // The read already drained the timerfd. Waking the entries that have come
        // due is all that is left, and resume_channel() does it.
        break;
    // A poll completion only reports that the fd became readable (its result is a
    // revents mask, not a byte count), and a notifier or signal read leaves the
    // reaping to the channel. There is nothing to write into a wait here.
    case Foundation::NBIO::ChannelType::kNotify:
    case Foundation::NBIO::ChannelType::kSignal:
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
            // Hand the outcome to the channel, which turns it into the answers of
            // the waits it covers, then let it wake them and do its own bookkeeping.
            complete(channel, cqe->res);
            resume_channel(channel);
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

    // This backend performs no data movement of its own: it runs a submission
    // phase and reports completions back into the channel. Registration only
    // needs to recognise the channel type.
    switch (channel->type())
    {
    case Foundation::NBIO::ChannelType::kReceive:
    case Foundation::NBIO::ChannelType::kSend:
    case Foundation::NBIO::ChannelType::kRead:
    case Foundation::NBIO::ChannelType::kWrite:
    case Foundation::NBIO::ChannelType::kAccept:
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
