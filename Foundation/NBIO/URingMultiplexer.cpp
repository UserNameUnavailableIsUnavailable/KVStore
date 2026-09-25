#if defined(__linux__)

#include "URingMultiplexer.hpp"

#include <Foundation/NBIO/Types.hpp>
#include <liburing.h>
#include <poll.h>
#include <sys/socket.h>

#include <Foundation/Core/TcpSocket.hpp>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <stdexcept>
#include <string>
#include <system_error>

#include "Channel.hpp"
#include "TcpAcceptChannel.hpp"
#include "TcpConnectChannel.hpp"
#include "RdmaAcceptChannel.hpp"
#include "RdmaConnectChannel.hpp"
#include "RdmaReceiveChannel.hpp"
#include "RdmaSendChannel.hpp"
#include "FileStream.hpp"
#include "FileReadChannel.hpp"
#include "EventNotifyChannel.hpp"
#include "TcpReceiveChannel.hpp"
#include "TcpSendChannel.hpp"
#include "SystemSignalChannel.hpp"
#include "FileWriteChannel.hpp"
#include "SystemTimerChannel.hpp"

#define MAKE_ERROR_CODE(e) std::error_code(e, std::system_category())

namespace Foundation::NBIO
{
#define ThrowUringError(error, what) \
    throw std::system_error(error, std::system_category(), std::string(what) + ": " + std::system_category().message(error));

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

void URingMultiplexer::add_channel(Foundation::NBIO::Channel *channel)
{
    // Registration is the whole of arming: the channel is asked for work until it
    // disarms.
    channels_.insert(channel);
}

void URingMultiplexer::delete_channel(Channel *channel) noexcept
{
    // TODO: cancel or drain the channel's in-flight operation before its frame
    // goes away; until then a completion naming a deleted channel is dropped.
    channels_.erase(channel);
    in_flight_.erase(channel);
}

// One completion's outcome into the batch it belongs to: the result is spread over
// the submissions the operation covered, in queue order.
static void advance(Foundation::NBIO::Channel *channel, int result)
{
    switch (channel->type())
    {
    case ChannelType::kReceive:
    {
        auto &payload = std::get<ReceivePayload>(static_cast<TcpReceiveChannel *>(channel)->submit());
        if (result < 0)
        {
            if (result == -EAGAIN || result == -EWOULDBLOCK)
            {
                break; // not ready is not an answer
            }
            const std::error_code failure{-result, std::system_category()};
            while (auto *submission = payload.next_submission())
            {
                submission->status = Core::OperationStatus::kError;
                submission->error_code = failure;
                payload.complete();
            }
        }
        else if (result == 0)
        {
            while (auto *submission = payload.next_submission())
            {
                submission->status = Core::OperationStatus::kDone;
                submission->bytes = 0;
                submission->error_code = {};
                payload.complete();
            }
        }
        else
        {
            std::size_t remaining = static_cast<std::size_t>(result);
            while (remaining > 0)
            {
                auto *submission = payload.next_submission();
                if (submission == nullptr)
                {
                    break;
                }
                const std::size_t taken = std::min(remaining, submission->buffer.size());
                submission->status = Core::OperationStatus::kDone;
                submission->bytes = taken;
                submission->error_code = {};
                payload.complete();
                remaining -= taken;
            }
        }
        break;
    }
    case ChannelType::kSend:
    {
        auto &payload = std::get<SendPayload>(static_cast<TcpSendChannel *>(channel)->submit());
        if (result < 0)
        {
            if (result == -EAGAIN || result == -EWOULDBLOCK)
            {
                break;
            }
            const std::error_code failure{-result, std::system_category()};
            while (auto *submission = payload.next_submission())
            {
                submission->status = Core::OperationStatus::kError;
                submission->error_code = failure;
                payload.complete();
            }
        }
        else if (result == 0)
        {
            auto *submission = payload.next_submission();
            if (submission != nullptr && !submission->buffer.empty())
            {
                submission->status = Core::OperationStatus::kError;
                submission->error_code = std::make_error_code(std::errc::io_error);
                payload.complete();
            }
        }
        else
        {
            std::size_t remaining = static_cast<std::size_t>(result);
            while (remaining > 0)
            {
                auto *submission = payload.next_submission();
                if (submission == nullptr)
                {
                    break;
                }
                const std::size_t size = submission->buffer.size();
                const std::size_t taken = std::min(remaining, size);
                submission->bytes += taken;
                remaining -= taken;
                if (taken == size)
                {
                    submission->status = Core::OperationStatus::kDone;
                    submission->error_code = {};
                    payload.complete();
                }
                else
                {
                    submission->buffer = submission->buffer.subspan(taken);
                    break;
                }
            }
        }
        break;
    }
    case ChannelType::kRead:
    {
        auto &payload = std::get<ReadPayload>(static_cast<FileReadChannel *>(channel)->submit());
        if (result < 0)
        {
            const std::error_code failure{-result, std::system_category()};
            while (auto *submission = payload.next_submission())
            {
                submission->status = Core::OperationStatus::kError;
                submission->error_code = failure;
                payload.complete();
            }
        }
        else
        {
            payload.advance_offset(static_cast<std::size_t>(result));
            std::size_t remaining = static_cast<std::size_t>(result);
            while (auto *submission = payload.next_submission())
            {
                if (remaining == 0)
                {
                    // A short read on a regular file is its end.
                    submission->status = Core::OperationStatus::kDone;
                    submission->bytes = 0;
                    submission->error_code = {};
                    payload.complete();
                    continue;
                }
                const std::size_t taken = std::min(remaining, submission->buffer.size());
                submission->status = Core::OperationStatus::kDone;
                submission->bytes = taken;
                submission->error_code = {};
                payload.complete();
                remaining -= taken;
            }
        }
        break;
    }
    case ChannelType::kWrite:
    {
        auto &payload = std::get<WritePayload>(static_cast<FileWriteChannel *>(channel)->submit());
        if (result < 0)
        {
            const std::error_code failure{-result, std::system_category()};
            while (auto *submission = payload.next_submission())
            {
                submission->status = Core::OperationStatus::kError;
                submission->error_code = failure;
                payload.complete();
            }
        }
        else
        {
            payload.advance_offset(static_cast<std::size_t>(result));
            std::size_t remaining = static_cast<std::size_t>(result);
            while (remaining > 0)
            {
                auto *submission = payload.next_submission();
                if (submission == nullptr)
                {
                    break;
                }
                const std::size_t size = submission->buffer.size();
                const std::size_t taken = std::min(remaining, size);
                submission->bytes += taken;
                remaining -= taken;
                if (taken == size)
                {
                    submission->status = Core::OperationStatus::kDone;
                    submission->error_code = {};
                    payload.complete();
                }
                else
                {
                    submission->buffer = submission->buffer.subspan(taken);
                    break;
                }
            }
        }
        break;
    }
    case ChannelType::kAccept:
    {
        auto &payload = std::get<AcceptPayload>(static_cast<TcpAcceptChannel *>(channel)->submit());
        auto *submission = payload.next_submission();
        if (submission == nullptr)
        {
            break;
        }
        if (result >= 0)
        {
            submission->status = Core::OperationStatus::kDone;
            submission->socket = Core::TcpSocket::adopt(static_cast<std::uintptr_t>(result));
            submission->error_code = {};
            payload.complete();
        }
        else if (result == -EAGAIN || result == -EWOULDBLOCK)
        {
            // Not ready is not an answer: the wait keeps its place.
        }
        else
        {
            submission->status = Core::OperationStatus::kError;
            submission->error_code = std::error_code(-result, std::system_category());
            payload.complete();
        }
        break;
    }
    // A poll completion only says the fd became readable; the channels that poll
    // reap their own events in complete().
    default:
        break;
    }
}

// Asks the channel to wake what a completion answered and arm what is still owed.
static void complete_channel(Foundation::NBIO::Channel *channel)
{
    switch (channel->type())
    {
    case ChannelType::kReceive:
        static_cast<TcpReceiveChannel *>(channel)->complete();
        break;
    case ChannelType::kSend:
        static_cast<TcpSendChannel *>(channel)->complete();
        break;
    case ChannelType::kRead:
        static_cast<FileReadChannel *>(channel)->complete();
        break;
    case ChannelType::kWrite:
        static_cast<FileWriteChannel *>(channel)->complete();
        break;
    case ChannelType::kAccept:
        static_cast<TcpAcceptChannel *>(channel)->complete();
        break;
    case ChannelType::kSystemTimer:
        static_cast<SystemTimerChannel *>(channel)->complete();
        break;
    case ChannelType::kNotify:
        static_cast<EventNotifyChannel *>(channel)->complete();
        break;
    case ChannelType::kSystemSignal:
        static_cast<SystemSignalChannel *>(channel)->complete();
        break;
    case ChannelType::kRdmaAccept:
        static_cast<RdmaAcceptChannel *>(channel)->complete();
        break;
    case ChannelType::kRdmaConnect:
        static_cast<RdmaConnectChannel *>(channel)->complete();
        break;
    case ChannelType::kConnect:
        static_cast<TcpConnectChannel *>(channel)->complete();
        break;
    case ChannelType::kRdmaSend:
        static_cast<RdmaSendChannel *>(channel)->complete();
        break;
    case ChannelType::kRdmaReceive:
        static_cast<RdmaReceiveChannel *>(channel)->complete();
        break;
    default:
        break;
    }
}

// Builds a one-shot poll for a poll-channel, whose whole wait is the poll. Which
// readiness that is belongs to the channel: most of them wait to be readable, a
// connect waits to be writable.
template <typename PollPayloadType>
static io_uring_sqe *prepare_poll_sqe(io_uring *ring, Foundation::NBIO::Channel *channel, PollPayloadType &payload,
                                      int mask = POLLIN)
{
    if (!payload.wants_poll())
    {
        return nullptr;
    }
    io_uring_sqe *sqe = ::io_uring_get_sqe(ring);
    if (sqe == nullptr)
    {
        return nullptr;
    }
    payload.take_poll();
    ::io_uring_prep_poll_add(sqe, channel->native_handle(), mask);
    return sqe;
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
    // One operation per channel is with the kernel at a time.
    if (in_flight_.contains(channel))
    {
        return false;
    }

    io_uring_sqe *sqe = nullptr;
    switch (channel->type())
    {
    case Foundation::NBIO::ChannelType::kReceive:
    {
        // Every prepared receive goes in one submission, and the kernel fills the
        // buffers in the order they were queued.
        auto *receive = static_cast<TcpReceiveChannel *>(channel);
        auto &payload = std::get<ReceivePayload>(receive->submit());
        if (payload.size() == 0)
        {
            return false; // nothing to receive for
        }
        auto &message = payload.header();
        sqe = ::io_uring_get_sqe(&ring_);
        if (sqe == nullptr)
        {
            return false;
        }
        ::io_uring_prep_recvmsg(sqe, receive->native_handle(), &message, 0);
        break;
    }
    case Foundation::NBIO::ChannelType::kSend:
    {
        // One sendmsg for the whole queue: a stream has to keep the order, so the
        // sends cannot be submitted as separate operations.
        auto *send = static_cast<TcpSendChannel *>(channel);
        auto &payload = std::get<SendPayload>(send->submit());
        if (payload.size() == 0)
        {
            return false; // nothing to send
        }
        auto &message = payload.header();
        sqe = ::io_uring_get_sqe(&ring_);
        if (sqe == nullptr)
        {
            return false;
        }
        ::io_uring_prep_sendmsg(sqe, send->native_handle(), &message, MSG_NOSIGNAL);
        break;
    }
    case Foundation::NBIO::ChannelType::kRead:
    {
        // Every prepared read goes in one submission: they cover consecutive
        // stretches of the file, so the kernel takes them as one vector.
        auto *read = static_cast<FileReadChannel *>(channel);
        auto &payload = std::get<ReadPayload>(read->submit());
        if (payload.size() == 0)
        {
            return false; // nothing to read for
        }
        auto &vectors = payload.header();
        sqe = ::io_uring_get_sqe(&ring_);
        if (sqe == nullptr)
        {
            return false;
        }
        ::io_uring_prep_readv(sqe, read->native_handle(), vectors.data(), static_cast<unsigned>(vectors.size()),
                              static_cast<__u64>(payload.offset()));
        break;
    }
    case Foundation::NBIO::ChannelType::kWrite:
    {
        // Every prepared write goes in one submission: they follow one another in
        // the file, so the kernel takes them as one vector.
        auto *write = static_cast<FileWriteChannel *>(channel);
        auto &payload = std::get<WritePayload>(write->submit());
        if (payload.size() == 0)
        {
            return false; // nothing to write
        }
        auto &vectors = payload.header();
        sqe = ::io_uring_get_sqe(&ring_);
        if (sqe == nullptr)
        {
            return false;
        }
        ::io_uring_prep_writev(sqe, write->native_handle(), vectors.data(), static_cast<unsigned>(vectors.size()),
                               static_cast<__u64>(payload.offset()));
        break;
    }
    case Foundation::NBIO::ChannelType::kAccept:
    {
        // Taking a connection cannot be vectorised, so one operation covers one
        // wait -- the wait at the front of the queue is the whole of what this ask
        // can hand over.
        auto *accept = static_cast<TcpAcceptChannel *>(channel);
        auto &payload = std::get<AcceptPayload>(accept->submit());
        if (payload.size() == 0)
        {
            return false; // nobody waiting
        }
        payload.bundle();
        auto *submission = payload.next_submission();
        if (submission == nullptr)
        {
            return false;
        }
        sqe = ::io_uring_get_sqe(&ring_);
        if (sqe == nullptr)
        {
            return false;
        }
        // The kernel writes the peer address straight into the waiting frame's
        // communication, which is alive for as long as that frame is parked.
        ::io_uring_prep_accept(sqe, accept->native_handle(), submission->address.storage(),
                               &submission->address.length(), 0);
        break;
    }
    // The channels that carry one wait move no data through the ring: a one-shot
    // poll is the entire wait, and they drain the descriptor themselves.
    case Foundation::NBIO::ChannelType::kSystemTimer:
    {
        auto &payload = std::get<SystemTimerPayload>(static_cast<SystemTimerChannel *>(channel)->submit());
        sqe = prepare_poll_sqe(&ring_, channel, payload);
        break;
    }
    case Foundation::NBIO::ChannelType::kNotify:
    {
        auto &payload = std::get<NotifyPayload>(static_cast<EventNotifyChannel *>(channel)->submit());
        sqe = prepare_poll_sqe(&ring_, channel, payload);
        break;
    }
    case Foundation::NBIO::ChannelType::kSystemSignal:
    {
        auto &payload = std::get<SystemSignalPayload>(static_cast<SystemSignalChannel *>(channel)->submit());
        sqe = prepare_poll_sqe(&ring_, channel, payload);
        break;
    }
    case Foundation::NBIO::ChannelType::kRdmaAccept:
    {
        auto &payload = std::get<RdmaAcceptPayload>(static_cast<RdmaAcceptChannel *>(channel)->submit());
        sqe = prepare_poll_sqe(&ring_, channel, payload);
        break;
    }
    case Foundation::NBIO::ChannelType::kRdmaConnect:
    {
        auto &payload = std::get<RdmaConnectPayload>(static_cast<RdmaConnectChannel *>(channel)->submit());
        sqe = prepare_poll_sqe(&ring_, channel, payload);
        break;
    }
    case Foundation::NBIO::ChannelType::kConnect:
    {
        auto &payload = std::get<ConnectPayload>(static_cast<TcpConnectChannel *>(channel)->submit());
        sqe = prepare_poll_sqe(&ring_, channel, payload, POLLOUT);
        break;
    }
    case Foundation::NBIO::ChannelType::kRdmaSend:
    {
        auto &payload = std::get<RdmaSendPayload>(static_cast<RdmaSendChannel *>(channel)->submit());
        sqe = prepare_poll_sqe(&ring_, channel, payload);
        break;
    }
    case Foundation::NBIO::ChannelType::kRdmaReceive:
    {
        auto &payload = std::get<RdmaReceivePayload>(static_cast<RdmaReceiveChannel *>(channel)->submit());
        sqe = prepare_poll_sqe(&ring_, channel, payload);
        break;
    }
    default:
        return false;
    }

    if (sqe == nullptr)
    {
        return false;
    }

    // The channel identifies itself; its type says which operation completed.
    ::io_uring_sqe_set_data(sqe, channel);
    in_flight_.insert(channel);
    return true;
}

void URingMultiplexer::submit()
{
    bool handed_over = false;
    for (Foundation::NBIO::Channel *channel : channels_)
    {
        handed_over = prepare(channel) || handed_over;
    }

    if (handed_over)
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
    // and only then is each channel asked to reap what it answered. A resumed
    // coroutine is free to prepare more work, and that must not happen while a
    // completion is still being written into the batch it is about to join.
    std::vector<Foundation::NBIO::Channel *> answered;
    answered.reserve(8);

    io_uring_cqe *cqe = nullptr;
    while (::io_uring_peek_cqe(&ring_, &cqe) == 0 && cqe != nullptr)
    {
        auto *channel = static_cast<Foundation::NBIO::Channel *>(::io_uring_cqe_get_data(cqe));
        if (channel != nullptr)
        {
            in_flight_.erase(channel);
            // A channel deleted while its operation was in flight is dropped: its
            // frame may already be gone.
            if (channels_.contains(channel))
            {
                advance(channel, cqe->res);
                if (std::find(answered.begin(), answered.end(), channel) == answered.end())
                {
                    answered.push_back(channel);
                }
            }
        }
        ::io_uring_cqe_seen(&ring_, cqe);
    }

    for (Foundation::NBIO::Channel *channel : answered)
    {
        complete_channel(channel);
    }
}


} // namespace Foundation::NBIO
#endif // defined(__linux__)
