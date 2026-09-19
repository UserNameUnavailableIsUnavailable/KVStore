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

// The channels that only poll hand their poll over and take it back through these
// two, which are defined below with the rest of the per-type switches.
static bool submit_poll(Foundation::NBIO::Channel *channel) noexcept;
static void complete_poll(Foundation::NBIO::Channel *channel) noexcept;

// One completion's outcome into the batch it belongs to: the channel spreads it
// over the jobs the operation covered.
static void advance(Foundation::NBIO::Channel *channel, int result)
{
    switch (channel->type())
    {
    case ChannelType::kReceive:
        static_cast<ReceiveChannel *>(channel)->advance_job(result);
        break;
    case ChannelType::kSend:
        static_cast<SendChannel *>(channel)->advance_job(result);
        break;
    case ChannelType::kRead:
        static_cast<ReadChannel *>(channel)->advance_job(result);
        break;
    case ChannelType::kWrite:
        static_cast<WriteChannel *>(channel)->advance_job(result);
        break;
    case ChannelType::kAccept:
        static_cast<AcceptChannel *>(channel)->advance_job(result);
        break;
    // A poll completion only says the fd became readable, and the channels that
    // poll reap their own events in handle_completion(): there is no wait for an
    // outcome to go in.
    default:
        break;
    }
}

// The batch is over for this channel: what the operation did not answer goes back
// to the front of its queue, ready for the next submission.
static void conclude(Foundation::NBIO::Channel *channel)
{
    switch (channel->type())
    {
    case ChannelType::kReceive:
        static_cast<ReceiveChannel *>(channel)->complete_jobs();
        break;
    case ChannelType::kSend:
        static_cast<SendChannel *>(channel)->complete_jobs();
        break;
    case ChannelType::kRead:
        static_cast<ReadChannel *>(channel)->complete_jobs();
        break;
    case ChannelType::kWrite:
        static_cast<WriteChannel *>(channel)->complete_jobs();
        break;
    case ChannelType::kAccept:
        static_cast<AcceptChannel *>(channel)->complete_jobs();
        break;
    // The channels that carry one wait are polled, and they reap what the poll means
    // themselves: their poll is over here, and each one may hand over another when it
    // still has something to watch.
    case ChannelType::kTimer:
    case ChannelType::kNotify:
    case ChannelType::kSignal:
    case ChannelType::kRDMA_Accept:
    case ChannelType::kRDMA_Connect:
    case ChannelType::kRDMA_Send:
    case ChannelType::kRDMA_Receive:
        complete_poll(channel);
        break;
    default:
        break;
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

// Asks a channel that only polls whether it has a poll to hand over, and records
// that it is out there. These channels reap their own events, so a poll is the
// whole wait -- and one at a time: a channel that already has one out there has
// nothing to hand over.
static bool submit_poll(Foundation::NBIO::Channel *channel) noexcept
{
    switch (channel->type())
    {
    case ChannelType::kTimer:
        return static_cast<TimerChannel *>(channel)->submit_job();
    case ChannelType::kNotify:
        return static_cast<NotifyChannel *>(channel)->submit_job();
    case ChannelType::kSignal:
        return static_cast<SignalChannel *>(channel)->submit_job();
    case ChannelType::kRDMA_Accept:
        return static_cast<RDMA_AcceptChannel *>(channel)->submit_job();
    case ChannelType::kRDMA_Connect:
        return static_cast<RDMA_ConnectChannel *>(channel)->submit_job();
    case ChannelType::kRDMA_Send:
        return static_cast<RDMA_SendChannel *>(channel)->submit_job();
    case ChannelType::kRDMA_Receive:
        return static_cast<RDMA_ReceiveChannel *>(channel)->submit_job();
    default:
        return false;
    }
}

// The poll is over: the channel may hand over another one when it has a wait to
// post again.
static void complete_poll(Foundation::NBIO::Channel *channel) noexcept
{
    switch (channel->type())
    {
    case ChannelType::kTimer:
        static_cast<TimerChannel *>(channel)->complete_job();
        break;
    case ChannelType::kNotify:
        static_cast<NotifyChannel *>(channel)->complete_job();
        break;
    case ChannelType::kSignal:
        static_cast<SignalChannel *>(channel)->complete_job();
        break;
    case ChannelType::kRDMA_Accept:
        static_cast<RDMA_AcceptChannel *>(channel)->complete_job();
        break;
    case ChannelType::kRDMA_Connect:
        static_cast<RDMA_ConnectChannel *>(channel)->complete_job();
        break;
    case ChannelType::kRDMA_Send:
        static_cast<RDMA_SendChannel *>(channel)->complete_job();
        break;
    case ChannelType::kRDMA_Receive:
        static_cast<RDMA_ReceiveChannel *>(channel)->complete_job();
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

bool URingMultiplexer::prepare(Foundation::NBIO::Channel *channel, std::size_t &unpublished)
{
    // An entry is only ever taken for an operation that is ready to be built in it,
    // and those two have to happen in that order. An entry taken from the ring and
    // then abandoned is not private: the next submit publishes it with whatever the
    // slot still holds, and the kernel runs that operation a second time -- an accept
    // nobody is waiting for, a send of a buffer that went out already. So the room
    // is made first and the channel is asked afterwards, which is also why an answer
    // of "nothing to hand over" costs nothing.
    //
    // The free-space query only counts what the kernel knows about, so the entries
    // this pass has filled in but not handed over yet are counted against it.
    if (::io_uring_sq_space_left(&ring_) <= unpublished)
    {
        // Handing over what is filled in is what makes room: the kernel takes them.
        if (::io_uring_submit(&ring_) <= 0)
        {
            return false; // the kernel did not take them: the caller retries later
        }
        unpublished = 0;

        if (::io_uring_sq_space_left(&ring_) == 0)
        {
            return false; // still full: the caller retries on a later iteration
        }
    }

    // The entry the operation is built in. The room for it is there, so this is one.
    const auto take_entry = [this]() noexcept { return ::io_uring_get_sqe(&ring_); };

    // Filled in by the one case that has an operation to build, which is also the
    // only case that takes an entry.
    io_uring_sqe *sqe = nullptr;

    switch (channel->type())
    {
    case Foundation::NBIO::ChannelType::kReceive: {
        // Every prepared receive goes in one submission, and the kernel fills the
        // buffers in the order they were queued.
        auto *receive = static_cast<ReceiveChannel *>(channel);
        ::msghdr *message = receive->submit_jobs();
        if (message == nullptr)
        {
            return false; // nothing to receive for
        }
        sqe = take_entry();
        ::io_uring_prep_recvmsg(sqe, receive->native_handle(), message, 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kSend: {
        // One sendmsg for the whole queue: a stream has to keep the order, so the
        // sends cannot be submitted as separate operations.
        auto *send = static_cast<SendChannel *>(channel);
        ::msghdr *message = send->submit_jobs();
        if (message == nullptr)
        {
            return false; // nothing to send
        }
        sqe = take_entry();
        ::io_uring_prep_sendmsg(sqe, send->native_handle(), message, MSG_NOSIGNAL);
        break;
    }
    case Foundation::NBIO::ChannelType::kRead: {
        // Every prepared read goes in one submission: they cover consecutive
        // stretches of the file, so the kernel takes them as one vector.
        auto *read = static_cast<ReadChannel *>(channel);
        const auto vectors = read->submit_jobs();
        if (vectors.empty())
        {
            return false; // nothing to read for
        }
        sqe = take_entry();
        ::io_uring_prep_readv(sqe, read->native_handle(), vectors.data(), static_cast<unsigned>(vectors.size()),
                              static_cast<__u64>(read->batch_offset()));
        break;
    }
    case Foundation::NBIO::ChannelType::kWrite: {
        // Every prepared write goes in one submission: they follow one another in
        // the file, so the kernel takes them as one vector.
        auto *write = static_cast<WriteChannel *>(channel);
        const auto vectors = write->submit_jobs();
        if (vectors.empty())
        {
            return false; // nothing to write
        }
        sqe = take_entry();
        ::io_uring_prep_writev(sqe, write->native_handle(), vectors.data(), static_cast<unsigned>(vectors.size()),
                               static_cast<__u64>(write->batch_offset()));
        break;
    }
    case Foundation::NBIO::ChannelType::kAccept: {
        // Taking a connection cannot be vectorised, so one operation covers one
        // wait -- the wait at the front of the queue is the whole of what this ask
        // can hand over.
        auto *accept = static_cast<AcceptChannel *>(channel);
        PendingAccept *waiting = accept->submit_jobs();
        if (waiting == nullptr)
        {
            return false; // nobody waiting, or a wait is still out there
        }
        sqe = take_entry();

        // Let the kernel write the peer address straight into the waiting frame's
        // result, which is alive for as long as that frame is parked.
        waiting->result().address.length() = waiting->result().address.capacity();
        ::io_uring_prep_accept(sqe, accept->native_handle(), waiting->result().address.storage<sockaddr>(),
                               &waiting->result().address.length(), 0);
        break;
    }
    // The channels that carry one wait move no data through the ring. They watch a
    // file descriptor -- a timer, a notifier, a signalfd, a connection-management
    // event, a work completion -- and then drain it themselves in
    // handle_completion(), so a one-shot poll is the entire wait. One at a time: a
    // channel that already has a poll out there has nothing to hand over.
    case Foundation::NBIO::ChannelType::kTimer:
    case Foundation::NBIO::ChannelType::kNotify:
    case Foundation::NBIO::ChannelType::kSignal:
    case Foundation::NBIO::ChannelType::kRDMA_Accept:
    case Foundation::NBIO::ChannelType::kRDMA_Connect:
    case Foundation::NBIO::ChannelType::kRDMA_Send:
    case Foundation::NBIO::ChannelType::kRDMA_Receive:
        if (!submit_poll(channel))
        {
            return false; // its poll is already out there
        }
        sqe = take_entry();
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
    // Entries this pass has filled in but not handed over yet: the ring's own
    // free-space query cannot see those until they are published.
    std::size_t unpublished = 0;
    std::vector<Foundation::NBIO::Channel *> still_waiting;
    still_waiting.reserve(pending_submissions_.size());

    for (Foundation::NBIO::Channel *channel : pending_submissions_)
    {
        // A channel may have more than one operation to hand over -- an accept
        // covers exactly one wait -- so it is asked until it has nothing left to
        // give: the batch it just submitted makes the next ask answer nothing, and
        // that answer leaves no entry behind.
        while (prepare(channel, unpublished))
        {
            ++handed_over;
            ++unpublished;
        }

        // It keeps its place whether it still has waits the submission queue could
        // not take or has a batch out there, because the next batch comes later.
        still_waiting.push_back(channel);
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

void URingMultiplexer::handle_completions()
{
    // A channel can have several completions in one pass -- an accept covers one
    // wait, and one wait is one operation -- so every completion is advanced first
    // and only then is each channel's batch concluded and its waiters woken. A
    // resumed coroutine is free to prepare more work, and that must not happen
    // while a completion is still being written into the batch it is about to
    // join.
    std::vector<Foundation::NBIO::Channel *> answered;
    answered.reserve(8);

    io_uring_cqe *cqe = nullptr;
    while (::io_uring_peek_cqe(&ring_, &cqe) == 0 && cqe != nullptr)
    {
        auto *channel = static_cast<Foundation::NBIO::Channel *>(::io_uring_cqe_get_data(cqe));
        if (channel != nullptr)
        {
            advance(channel, cqe->res);
            if (std::find(answered.begin(), answered.end(), channel) == answered.end())
            {
                answered.push_back(channel);
            }
        }
        ::io_uring_cqe_seen(&ring_, cqe);
    }

    for (Foundation::NBIO::Channel *channel : answered)
    {
        conclude(channel);
        resume_channel(channel);
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
