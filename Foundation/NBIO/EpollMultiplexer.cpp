#if defined(__linux__)
#include "EpollMultiplexer.hpp"

#include <cassert>
#include <spdlog/spdlog.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <stdexcept>
#include <system_error>
#include <sys/uio.h>
#include <utility>

#include "EventNotifyChannel.hpp"
#include "SystemSignalChannel.hpp"
#include "Types.hpp"
#include "Channel.hpp"
#include "Multiplexer.hpp"
#include "FileStream.hpp"
#include "TcpAcceptChannel.hpp"
#include "TcpConnectChannel.hpp"
#include "FileReadChannel.hpp"
#include "TcpReceiveChannel.hpp"
#include "TcpSendChannel.hpp"
#include "SystemTimerChannel.hpp"
#include "FileWriteChannel.hpp"
#include "RdmaConnectChannel.hpp"
#include "RdmaAcceptChannel.hpp"
#include "RdmaSendChannel.hpp"
#include "RdmaReceiveChannel.hpp"

namespace Foundation::NBIO
{
static void do_receive_data(Foundation::NBIO::Channel *base);
static void do_send_data(Foundation::NBIO::Channel *base);
static void do_accept_connection(Foundation::NBIO::Channel *base);
static void do_read_file(Foundation::NBIO::Channel *base);
static void do_write_file(Foundation::NBIO::Channel *base);

EpollMultiplexer::EpollMultiplexer() :
	Multiplexer(MultiplexerType::kEpoll)
{
    handle_ = epoll_create1(0);
    if (handle_ < 0)
    {
        throw std::runtime_error("epoll_create1 failed");
    }
}

static bool is_always_ready(ChannelType type)
{
	return type == ChannelType::kRead || type == ChannelType::kWrite;
}

static constexpr std::uint32_t native_flags_for(Foundation::NBIO::ChannelType type)
{
    switch (type)
    {
    case Foundation::NBIO::ChannelType::kReceive:
    case Foundation::NBIO::ChannelType::kRead:
    case Foundation::NBIO::ChannelType::kAccept:
    case Foundation::NBIO::ChannelType::kSystemTimer:
    case Foundation::NBIO::ChannelType::kNotify:
    case Foundation::NBIO::ChannelType::kRdmaAccept:
    case Foundation::NBIO::ChannelType::kRdmaConnect:
    case Foundation::NBIO::ChannelType::kRdmaSend:
    case Foundation::NBIO::ChannelType::kRdmaReceive:
    case Foundation::NBIO::ChannelType::kSystemSignal:
        return EPOLLIN;
    // A connect is finished the moment the socket will take bytes: writability is
    // the readiness that means the handshake is over.
    case Foundation::NBIO::ChannelType::kConnect:
        return EPOLLOUT;
    case Foundation::NBIO::ChannelType::kSend:
    case Foundation::NBIO::ChannelType::kWrite:
        return EPOLLOUT;
    default:
        throw std::logic_error("EpollMultiplexer::active_flags_for: unsupported channel type");
    }
}

// run once
void EpollMultiplexer::run_impl(int timeout_ms)
{
    std::array<::epoll_event, 4096> events;
    bool retry = false;
    do
    {
        retry = false;

        // Always-ready channels (regular files) are processed every pass.
        for (const auto &[fd, channel] : always_channels_)
        {
            active_channels_.push_back(channel);
        }
        if (!active_channels_.empty())
        {
            timeout_ms = 0; // do not block: poll once and return
        }

        auto n = epoll_wait(handle_, events.data(), events.size(), timeout_ms);
        if (n < 0)
        {
            if (errno == EINTR)
            {
                retry = true;
                continue; // retry
            }
            throw std::system_error(errno, std::system_category(), "epoll_wait failed");
        }

        for (auto i = 0; i < n; ++i)
        {
            const auto fd = events[i].data.fd;
            const auto ev = events[i].events;

            // epoll reports ERR/HUP regardless of the requested interest, and
            // they do not match EPOLLIN bit-wise -- count them as readable, or
            // a failing connection would never wake its receive channel.
            auto [begin, end] = pollable_channels_.equal_range(fd);
            for (auto it = begin; it != end; ++it)
            {
                auto *channel = it->second;
                const auto flags = native_flags_for(channel->type()) | EPOLLHUP | EPOLLERR;
                if ((flags & ev) != 0)
                {
                    active_channels_.push_back(channel);
                }
            }
        }

        // The backend does what only it can do -- the I/O itself -- and then the
        // channel reaps the payload the backend filled: waking the answered
        // waiters and arming again while anything is still waiting.
        for (auto *channel : active_channels_)
        {
            switch (channel->type())
            {
            case Foundation::NBIO::ChannelType::kReceive:
                do_receive_data(channel);
                static_cast<TcpReceiveChannel *>(channel)->complete();
                break;
            case Foundation::NBIO::ChannelType::kSend:
                do_send_data(channel);
                static_cast<TcpSendChannel *>(channel)->complete();
                break;
            case Foundation::NBIO::ChannelType::kAccept:
                do_accept_connection(channel);
                static_cast<TcpAcceptChannel *>(channel)->complete();
                break;
            case Foundation::NBIO::ChannelType::kRead:
                do_read_file(channel);
                static_cast<FileReadChannel *>(channel)->complete();
                break;
            case Foundation::NBIO::ChannelType::kWrite:
                do_write_file(channel);
                static_cast<FileWriteChannel *>(channel)->complete();
                break;
            // Readiness is the whole wait for these: each one drains its fd itself.
            case Foundation::NBIO::ChannelType::kSystemTimer:
                static_cast<SystemTimerChannel *>(channel)->complete();
                break;
            case Foundation::NBIO::ChannelType::kNotify:
                static_cast<EventNotifyChannel *>(channel)->complete();
                break;
            case Foundation::NBIO::ChannelType::kSystemSignal:
                static_cast<SystemSignalChannel *>(channel)->complete();
                break;
            case Foundation::NBIO::ChannelType::kRdmaAccept:
                static_cast<RdmaAcceptChannel *>(channel)->complete();
                break;
            case Foundation::NBIO::ChannelType::kRdmaConnect:
                static_cast<RdmaConnectChannel *>(channel)->complete();
                break;
            // Nothing to read and nothing to write: the readiness is the whole of the
            // completion, and the channel is the one that knows what to ask the
            // socket about it.
            case Foundation::NBIO::ChannelType::kConnect:
                static_cast<TcpConnectChannel *>(channel)->complete();
                break;
            case Foundation::NBIO::ChannelType::kRdmaSend:
                static_cast<RdmaSendChannel *>(channel)->complete();
                break;
            case Foundation::NBIO::ChannelType::kRdmaReceive:
                static_cast<RdmaReceiveChannel *>(channel)->complete();
                break;
            }
        }
        active_channels_.clear();
    } while (retry);
}

void EpollMultiplexer::run()
{
    run_impl(-1);
}

void EpollMultiplexer::run_for(std::chrono::milliseconds timeout)
{
    auto now = std::chrono::steady_clock::now();
    auto due = now + timeout;
    do
    {
        auto remaining = (due - now).count();
        auto ms = remaining > INT_MAX ? INT_MAX : remaining;
        run_impl(static_cast<int>(ms));
        now = std::chrono::steady_clock::now();
    } while (now < due);
}

void EpollMultiplexer::add_channel(Foundation::NBIO::Channel *channel)
{
    // Registration is the whole of arming: every channel is dedicated to one
    // event, so there is nothing to update -- only to add and to remove.
    const int fd = static_cast<int>(channel->native_handle());

    if (is_always_ready(channel->type()))
    {
        auto [begin, end] = always_channels_.equal_range(fd);
        for (auto it = begin; it != end; ++it)
        {
            if (it->second == channel) [[unlikely]]
            {
                return; // already added
            }
        }
        always_channels_.emplace(fd, channel);
        return;
    }

    auto [begin, end] = pollable_channels_.equal_range(fd);
    for (auto it = begin; it != end; ++it)
    {
        if (it->second == channel) [[unlikely]]
        {
            return; // already added
        }
    }

    if (begin == end)
    {
        // The first channel on this fd registers it.
        ::epoll_event event{.events = native_flags_for(channel->type()), .data{.fd = fd}};
        if (::epoll_ctl(handle_, EPOLL_CTL_ADD, fd, &event) != 0)
        {
            throw std::system_error(errno, std::system_category(), "epoll_ctl(ADD) failed");
        }
    }
    else
    {
        // Simplex channels share a socket: the interest is the union of them all.
        std::uint32_t flags = native_flags_for(channel->type());
        for (auto it = begin; it != end; ++it)
        {
            flags |= native_flags_for(it->second->type());
        }
        ::epoll_event event{.events = flags, .data{.fd = fd}};
        if (::epoll_ctl(handle_, EPOLL_CTL_MOD, fd, &event) != 0)
        {
            throw std::system_error(errno, std::system_category(), "epoll_ctl(MOD) failed");
        }
    }
    pollable_channels_.emplace(fd, channel);
}

void EpollMultiplexer::delete_channel(Foundation::NBIO::Channel *channel) noexcept
{
    const int fd = static_cast<int>(channel->native_handle());

    if (is_always_ready(channel->type()))
    {
        auto [begin, end] = always_channels_.equal_range(fd);
        for (auto it = begin; it != end; ++it)
        {
            if (it->second == channel)
            {
                always_channels_.erase(it);
                return;
            }
        }
        return;
    }

    {
        auto [begin, end] = pollable_channels_.equal_range(fd);
        auto target = end;
        for (auto it = begin; it != end; ++it)
        {
            if (it->second == channel)
            {
                target = it;
                break;
            }
        }
        if (target == end)
        {
            return; // never registered
        }
        pollable_channels_.erase(target);
    }

    auto [begin, end] = pollable_channels_.equal_range(fd);
    if (begin == end)
    {
        ::epoll_ctl(handle_, EPOLL_CTL_DEL, fd, nullptr);
        return;
    }

    std::uint32_t flags = 0;
    for (auto it = begin; it != end; ++it)
    {
        flags |= native_flags_for(it->second->type());
    }
    ::epoll_event event{.events = flags, .data{.fd = fd}};
    ::epoll_ctl(handle_, EPOLL_CTL_MOD, fd, &event);
}

EpollMultiplexer::~EpollMultiplexer() noexcept
{
    close(handle_);
}

static void do_receive_data(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<TcpReceiveChannel *>(base);
    auto &payload = std::get<ReceivePayload>(channel->submit());
    if (payload.size() == 0)
    {
        return; // nothing to receive for
    }

    auto &message = payload.header();
    ssize_t result = 0;
    do
    {
        result = ::recvmsg(channel->native_handle(), &message, 0);
    } while (result < 0 && errno == EINTR);

    if (result < 0)
    {
        const int error = errno;
        if (error == EAGAIN || error == EWOULDBLOCK)
        {
            // Not ready is not an answer: every receive in the batch keeps waiting.
            return;
        }
        const std::error_code failure{error, std::system_category()};
        while (auto *submission = payload.next_submission())
        {
            submission->status = Core::OperationStatus::kError;
            submission->error_code = failure;
            payload.complete();
        }
        return;
    }

    if (result == 0)
    {
        // The peer closed, and it closed for every receive waiting.
        while (auto *submission = payload.next_submission())
        {
            submission->status = Core::OperationStatus::kDone;
            submission->bytes = 0;
            submission->error_code = {};
            payload.complete();
        }
        return;
    }

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

static void do_send_data(Channel *base)
{
    auto *channel = static_cast<TcpSendChannel *>(base);
    auto &payload = std::get<SendPayload>(channel->submit());
    if (payload.size() == 0)
    {
        return; // nothing to send
    }

    auto &message = payload.header();
    ssize_t result = 0;
    bool retry{false};
    bool failed{false};
    do
    {
        retry = false;
        result = ::sendmsg(channel->native_handle(), &message, MSG_NOSIGNAL);
        if (result < 0)
        {
            if (errno == EINTR)
            {
                retry = true;
            }
            else if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return; // not writable: keep waiting
            }
            else
            {
                failed = true;
            }
        }
    } while (retry);

    if (failed) [[unlikely]]
    {
        // A socket that cannot be written is one socket: every send fails.
        std::error_code failure{errno, std::system_category()};
        while (auto *submission = payload.next_submission())
        {
            submission->status = Core::OperationStatus::kError;
            submission->error_code = failure;
            payload.complete();
        }
        return;
    }

    if (result == 0)
    {
        // Nothing was taken from bytes that were there to send; asking again would
        // spin, so the front is failed instead.
        auto *submission = payload.next_submission();
        if (submission != nullptr && !submission->buffer.empty())
        {
            submission->status = Core::OperationStatus::kError;
            submission->error_code = std::make_error_code(std::errc::io_error);
            payload.complete();
        }
        return;
    }

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
            // The operation stopped inside this send: what is left of it goes first
            // next time, and it keeps its place.
            submission->buffer = submission->buffer.subspan(taken);
            break;
        }
    }
}

static void do_accept_connection(Channel *base)
{
    auto *channel = static_cast<TcpAcceptChannel *>(base);
    auto &payload = std::get<AcceptPayload>(channel->submit());
    if (payload.size() == 0)
    {
        return; // nobody is waiting
    }
    payload.bundle();

    bool stop{false};
    bool failed{false};
    while (!failed && !stop)
    {
        auto *submission = payload.next_submission();
        if (submission == nullptr)
        {
            break;
        }
        bool retry{false};
        do
        {
            const auto fd = ::accept(channel->native_handle(), submission->address.storage(), &submission->address.length());
            if (fd == -1)
            {
                if (errno == EINTR) [[unlikely]]
                {
                    retry = true;
                }
                else if (errno == EAGAIN || errno == EWOULDBLOCK) [[likely]]
                {
                    stop = true;
                }
                else [[unlikely]]
                {
                    failed = true;
                }
            }
            else
            {
                submission->status = Core::OperationStatus::kDone;
                submission->socket = Core::TcpSocket::adopt(static_cast<std::uintptr_t>(fd));
                submission->error_code = {};
                payload.complete();
            }
        } while (retry);
    }

    if (failed)
    {
        // The listener failed: every wait behind the front fails with it.
        std::error_code failure{errno, std::system_category()};
        while (auto *submission = payload.next_submission())
        {
            submission->status = Core::OperationStatus::kError;
            submission->error_code = failure;
            payload.complete();
        }
    }
}

static void do_read_file(Foundation::NBIO::Channel *base)
{
    // A file is always ready, so the batch the channel hands over here is the
    // operation: the reads cover consecutive stretches of it.
    auto *channel = static_cast<FileReadChannel *>(base);
    auto &payload = std::get<ReadPayload>(channel->submit());
    if (payload.size() == 0)
    {
        return; // nothing to read for
    }

    auto &vectors = payload.header();
    ssize_t result = 0;
    do
    {
        result = ::preadv(channel->native_handle(), vectors.data(), static_cast<int>(vectors.size()),
                          static_cast<off_t>(payload.offset()));
    } while (result < 0 && errno == EINTR);

    if (result < 0)
    {
        const int error = errno;
        const std::error_code failure{error, std::system_category()};
        while (auto *submission = payload.next_submission())
        {
            submission->status = Core::OperationStatus::kError;
            submission->error_code = failure;
            payload.complete();
        }
        return;
    }

    payload.advance_offset(static_cast<std::size_t>(result));
    std::size_t remaining = static_cast<std::size_t>(result);
    while (auto *submission = payload.next_submission())
    {
        if (remaining == 0)
        {
            // A short read on a regular file is its end: the reads behind it would
            // read at or past where it stopped.
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

static void do_write_file(Foundation::NBIO::Channel *base)
{
    // As for reading: the batch is the operation, and the writes follow one another
    // in the file.
    auto *channel = static_cast<FileWriteChannel *>(base);
    auto &payload = std::get<WritePayload>(channel->submit());
    if (payload.size() == 0)
    {
        return; // nothing to write
    }

    auto &vectors = payload.header();
    ssize_t result = 0;
    do
    {
        result = ::pwritev(channel->native_handle(), vectors.data(), static_cast<int>(vectors.size()),
                           static_cast<off_t>(payload.offset()));
    } while (result < 0 && errno == EINTR);

    if (result < 0)
    {
        const int error = errno;
        const std::error_code failure{error, std::system_category()};
        while (auto *submission = payload.next_submission())
        {
            submission->status = Core::OperationStatus::kError;
            submission->error_code = failure;
            payload.complete();
        }
        return;
    }

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
            // The operation stopped inside this write: what is left of it keeps its
            // place in the file.
            submission->buffer = submission->buffer.subspan(taken);
            break;
        }
    }
}

} // namespace Foundation::NBIO
#endif // defined(__linux__)
