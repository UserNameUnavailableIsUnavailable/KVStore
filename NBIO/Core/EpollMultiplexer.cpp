#if defined(__linux__)
#include "EpollMultiplexer.hpp"

#include <spdlog/spdlog.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/unistd.h>

#include <array>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <climits>
#include <NBIO/Net/TcpSocket.hpp>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>

#include <NBIO/Core/Channel.hpp>
#include <NBIO/Notification/EventNotifyChannel.hpp>
#include <NBIO/FS/FileReadChannel.hpp>
#include <NBIO/FS/FileStream.hpp>
#include <NBIO/FS/FileWriteChannel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <NBIO/RDMA/RdmaAcceptChannel.hpp>
#include <NBIO/RDMA/RdmaConnectChannel.hpp>
#include <NBIO/RDMA/RdmaReceiveChannel.hpp>
#include <NBIO/RDMA/RdmaSendChannel.hpp>
#include <NBIO/Signal/SystemSignalChannel.hpp>
#include <NBIO/Time/SystemTimerChannel.hpp>
#include <NBIO/Net/TcpAcceptChannel.hpp>
#include <NBIO/Net/TcpConnectChannel.hpp>
#include <NBIO/Net/TcpReceiveChannel.hpp>
#include <NBIO/Net/TcpSendChannel.hpp>
#include <NBIO/Core/Types.hpp>

namespace NBIO::Core {
using namespace NBIO::FS;
using namespace NBIO::Net;
using namespace NBIO::Notification;
using namespace NBIO::RDMA;
using namespace NBIO::Signal;
using namespace NBIO::Time;

using ChannelVariant =
    std::variant<TcpReceiveChannel*, TcpSendChannel*, TcpAcceptChannel*, FileReadChannel*, FileWriteChannel*,
                 SystemTimerChannel*, EventNotifyChannel*, SystemSignalChannel*, RdmaAcceptChannel*,
                 RdmaConnectChannel*, TcpConnectChannel*, RdmaSendChannel*, RdmaReceiveChannel*>;

static ChannelVariant as_variant(ChannelBase* channel) {
    switch (channel->type()) {
        case ChannelType::kReceive:
            return static_cast<TcpReceiveChannel*>(channel);
        case ChannelType::kSend:
            return static_cast<TcpSendChannel*>(channel);
        case ChannelType::kAccept:
            return static_cast<TcpAcceptChannel*>(channel);
        case ChannelType::kRead:
            return static_cast<FileReadChannel*>(channel);
        case ChannelType::kWrite:
            return static_cast<FileWriteChannel*>(channel);
        case ChannelType::kSystemTimer:
            return static_cast<SystemTimerChannel*>(channel);
        case ChannelType::kNotify:
            return static_cast<EventNotifyChannel*>(channel);
        case ChannelType::kSystemSignal:
            return static_cast<SystemSignalChannel*>(channel);
        case ChannelType::kRdmaAccept:
            return static_cast<RdmaAcceptChannel*>(channel);
        case ChannelType::kRdmaConnect:
            return static_cast<RdmaConnectChannel*>(channel);
        case ChannelType::kConnect:
            return static_cast<TcpConnectChannel*>(channel);
        case ChannelType::kRdmaSend:
            return static_cast<RdmaSendChannel*>(channel);
        case ChannelType::kRdmaReceive:
            return static_cast<RdmaReceiveChannel*>(channel);
    }
    throw std::logic_error("EpollMultiplexer::as_variant: unsupported channel type");
}

template <typename Visitor>
static decltype(auto) visit_channel(ChannelBase* channel, Visitor&& visitor) {
    return std::visit(std::forward<Visitor>(visitor), as_variant(channel));
}

static void do_receive_data(TcpReceiveChannel* channel);
static void do_send_data(TcpSendChannel* channel);
static void do_accept_connection(TcpAcceptChannel* channel);
static void do_read_file(FileReadChannel* channel);
static void do_write_file(FileWriteChannel* channel);

template <typename T>
static void complete_channel(T* channel) {
    using ChannelType = std::remove_pointer_t<T>;

    if constexpr (std::is_same_v<ChannelType, TcpReceiveChannel>) {
        do_receive_data(channel);
        channel->complete();
    } else if constexpr (std::is_same_v<ChannelType, TcpSendChannel>) {
        do_send_data(channel);
        channel->complete();
    } else if constexpr (std::is_same_v<ChannelType, TcpAcceptChannel>) {
        do_accept_connection(channel);
        channel->complete();
    } else if constexpr (std::is_same_v<ChannelType, FileReadChannel>) {
        do_read_file(channel);
        channel->complete();
    } else if constexpr (std::is_same_v<ChannelType, FileWriteChannel>) {
        do_write_file(channel);
        channel->complete();
    } else {
        channel->complete();
    }
}

EpollMultiplexer::EpollMultiplexer() : Multiplexer(MultiplexerType::kEpoll) {
    handle_ = epoll_create1(0);
    if (handle_ < 0) {
        throw std::runtime_error("epoll_create1 failed");
    }
}

static bool is_always_ready(ChannelType type) { return type == ChannelType::kRead || type == ChannelType::kWrite; }

static constexpr std::uint32_t native_flags_for(NBIO::Core::ChannelType type) {
    switch (type) {
        case NBIO::Core::ChannelType::kReceive:
        case NBIO::Core::ChannelType::kRead:
        case NBIO::Core::ChannelType::kAccept:
        case NBIO::Core::ChannelType::kSystemTimer:
        case NBIO::Core::ChannelType::kNotify:
        case NBIO::Core::ChannelType::kRdmaAccept:
        case NBIO::Core::ChannelType::kRdmaConnect:
        case NBIO::Core::ChannelType::kRdmaSend:
        case NBIO::Core::ChannelType::kRdmaReceive:
        case NBIO::Core::ChannelType::kSystemSignal:
            return EPOLLIN;
        // A connect is finished the moment the socket will take bytes: writability is
        // the readiness that means the handshake is over.
        case NBIO::Core::ChannelType::kConnect:
            return EPOLLOUT;
        case NBIO::Core::ChannelType::kSend:
        case NBIO::Core::ChannelType::kWrite:
            return EPOLLOUT;
        default:
            throw std::logic_error("EpollMultiplexer::active_flags_for: unsupported channel type");
    }
}

// run once
void EpollMultiplexer::run_impl(int timeout_ms) {
    std::array<::epoll_event, 4096> events;
    bool retry = false;
    do {
        retry = false;

        // Always-ready channels (regular files) are processed every pass.
        for (const auto& [fd, channel] : always_channels_) {
            active_channels_.push_back(channel);
        }
        if (!active_channels_.empty()) {
            timeout_ms = 0;  // do not block: poll once and return
        }

        auto n = epoll_wait(handle_, events.data(), events.size(), timeout_ms);
        if (n < 0) {
            if (errno == EINTR) {
                retry = true;
                continue;  // retry
            }
            throw std::system_error(errno, std::system_category(), "epoll_wait failed");
        }

        for (auto i = 0; i < n; ++i) {
            const auto fd = events[i].data.fd;
            const auto ev = events[i].events;

            // epoll reports ERR/HUP regardless of the requested interest, and
            // they do not match EPOLLIN bit-wise -- count them as readable, or
            // a failing connection would never wake its receive channel.
            auto [begin, end] = pollable_channels_.equal_range(fd);
            for (auto it = begin; it != end; ++it) {
                auto* channel = it->second;
                const auto flags = native_flags_for(channel->type()) | EPOLLHUP | EPOLLERR;
                if ((flags & ev) != 0) {
                    active_channels_.push_back(channel);
                }
            }
        }

        // The backend does what only it can do -- the I/O itself -- and then the
        // channel reaps the payload the backend filled: waking the answered
        // waiters and arming again while anything is still waiting.
        for (auto* channel : active_channels_) {
            visit_channel(channel, [](auto* typed) { complete_channel(typed); });
        }
        active_channels_.clear();
    } while (retry);
}

void EpollMultiplexer::run() { run_impl(-1); }

void EpollMultiplexer::run_for(std::chrono::milliseconds timeout) {
    auto now = std::chrono::steady_clock::now();
    auto due = now + timeout;
    do {
        auto remaining = (due - now).count();
        auto ms = remaining > INT_MAX ? INT_MAX : remaining;
        run_impl(static_cast<int>(ms));
        now = std::chrono::steady_clock::now();
    } while (now < due);
}

void EpollMultiplexer::add_channel(NBIO::Core::ChannelBase* channel) {
    // Registration is the whole of arming: every channel is dedicated to one
    // event, so there is nothing to update -- only to add and to remove.
    const int fd = static_cast<int>(channel->native_handle());

    if (is_always_ready(channel->type())) {
        auto [begin, end] = always_channels_.equal_range(fd);
        for (auto it = begin; it != end; ++it) {
            if (it->second == channel) [[unlikely]] {
                return;  // already added
            }
        }
        always_channels_.emplace(fd, channel);
        return;
    }

    auto [begin, end] = pollable_channels_.equal_range(fd);
    for (auto it = begin; it != end; ++it) {
        if (it->second == channel) [[unlikely]] {
            return;  // already added
        }
    }

    if (begin == end) {
        // The first channel on this fd registers it.
        ::epoll_event event{.events = native_flags_for(channel->type()), .data{.fd = fd}};
        if (::epoll_ctl(handle_, EPOLL_CTL_ADD, fd, &event) != 0) {
            throw std::system_error(errno, std::system_category(), "epoll_ctl(ADD) failed");
        }
    } else {
        // Simplex channels share a socket: the interest is the union of them all.
        std::uint32_t flags = native_flags_for(channel->type());
        for (auto it = begin; it != end; ++it) {
            flags |= native_flags_for(it->second->type());
        }
        ::epoll_event event{.events = flags, .data{.fd = fd}};
        if (::epoll_ctl(handle_, EPOLL_CTL_MOD, fd, &event) != 0) {
            throw std::system_error(errno, std::system_category(), "epoll_ctl(MOD) failed");
        }
    }
    pollable_channels_.emplace(fd, channel);
}

void EpollMultiplexer::delete_channel(NBIO::Core::ChannelBase* channel) noexcept {
    const int fd = static_cast<int>(channel->native_handle());

    if (is_always_ready(channel->type())) {
        auto [begin, end] = always_channels_.equal_range(fd);
        for (auto it = begin; it != end; ++it) {
            if (it->second == channel) {
                always_channels_.erase(it);
                return;
            }
        }
        return;
    }

    {
        auto [begin, end] = pollable_channels_.equal_range(fd);
        auto target = end;
        for (auto it = begin; it != end; ++it) {
            if (it->second == channel) {
                target = it;
                break;
            }
        }
        if (target == end) {
            return;  // never registered
        }
        pollable_channels_.erase(target);
    }

    auto [begin, end] = pollable_channels_.equal_range(fd);
    if (begin == end) {
        ::epoll_ctl(handle_, EPOLL_CTL_DEL, fd, nullptr);
        return;
    }

    std::uint32_t flags = 0;
    for (auto it = begin; it != end; ++it) {
        flags |= native_flags_for(it->second->type());
    }
    ::epoll_event event{.events = flags, .data{.fd = fd}};
    ::epoll_ctl(handle_, EPOLL_CTL_MOD, fd, &event);
}

EpollMultiplexer::~EpollMultiplexer() noexcept { close(handle_); }

static void do_receive_data(TcpReceiveChannel* channel) {
    auto& payload = channel->submit();
    if (payload.size() == 0) {
        return;  // nothing to receive for
    }

    auto& message = payload.header();
    ssize_t result = 0;
    do {
        result = ::recvmsg(channel->native_handle(), &message, 0);
    } while (result < 0 && errno == EINTR);

    if (result < 0) {
        const int error = errno;
        if (error == EAGAIN || error == EWOULDBLOCK) {
            // Not ready is not an answer: every receive in the batch keeps waiting.
            return;
        }
        const std::error_code failure{error, std::system_category()};
        while (auto* submission = payload.next_submission()) {
            submission->status = Net::OperationStatus::kError;
            submission->error_code = failure;
            payload.complete();
        }
        return;
    }

    if (result == 0) {
        // The peer closed, and it closed for every receive waiting.
        while (auto* submission = payload.next_submission()) {
            submission->status = Net::OperationStatus::kDone;
            submission->bytes = 0;
            submission->error_code = {};
            payload.complete();
        }
        return;
    }

    std::size_t remaining = static_cast<std::size_t>(result);
    while (remaining > 0) {
        auto* submission = payload.next_submission();
        if (submission == nullptr) {
            break;
        }
        const std::size_t taken = std::min(remaining, submission->buffer.size());
        submission->status = Net::OperationStatus::kDone;
        submission->bytes = taken;
        submission->error_code = {};
        payload.complete();
        remaining -= taken;
    }
}

static void do_send_data(TcpSendChannel* channel) {
    auto& payload = channel->submit();
    if (payload.size() == 0) {
        return;  // nothing to send
    }

    auto& message = payload.header();
    ssize_t result = 0;
    bool retry{false};
    bool failed{false};
    do {
        retry = false;
        result = ::sendmsg(channel->native_handle(), &message, MSG_NOSIGNAL);
        if (result < 0) {
            if (errno == EINTR) {
                retry = true;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;  // not writable: keep waiting
            } else {
                failed = true;
            }
        }
    } while (retry);

    if (failed) [[unlikely]] {
        // A socket that cannot be written is one socket: every send fails.
        std::error_code failure{errno, std::system_category()};
        while (auto* submission = payload.next_submission()) {
            submission->status = Net::OperationStatus::kError;
            submission->error_code = failure;
            payload.complete();
        }
        return;
    }

    if (result == 0) {
        // Nothing was taken from bytes that were there to send; asking again would
        // spin, so the front is failed instead.
        auto* submission = payload.next_submission();
        if (submission != nullptr && !submission->buffer.empty()) {
            submission->status = Net::OperationStatus::kError;
            submission->error_code = std::make_error_code(std::errc::io_error);
            payload.complete();
        }
        return;
    }

    std::size_t remaining = static_cast<std::size_t>(result);
    while (remaining > 0) {
        auto* submission = payload.next_submission();
        if (submission == nullptr) {
            break;
        }
        const std::size_t size = submission->buffer.size();
        const std::size_t taken = std::min(remaining, size);
        submission->bytes += taken;
        remaining -= taken;

        if (taken == size) {
            submission->status = Net::OperationStatus::kDone;
            submission->error_code = {};
            payload.complete();
        } else {
            // The operation stopped inside this send: what is left of it goes first
            // next time, and it keeps its place.
            submission->buffer = submission->buffer.subspan(taken);
            break;
        }
    }
}

static void do_accept_connection(TcpAcceptChannel* channel) {
    auto& payload = channel->submit();
    if (payload.size() == 0) {
        return;  // nobody is waiting
    }
    payload.bundle();

    bool stop{false};
    bool failed{false};
    while (!failed && !stop) {
        auto* submission = payload.next_submission();
        if (submission == nullptr) {
            break;
        }
        bool retry{false};
        do {
            const auto fd =
                ::accept(channel->native_handle(), submission->address.storage(), &submission->address.length());
            if (fd == -1) {
                if (errno == EINTR) [[unlikely]] {
                    retry = true;
                } else if (errno == EAGAIN || errno == EWOULDBLOCK) [[likely]] {
                    stop = true;
                } else [[unlikely]] {
                    failed = true;
                }
            } else {
                submission->status = Net::OperationStatus::kDone;
                submission->socket = Net::TcpSocket::adopt(static_cast<std::uintptr_t>(fd));
                submission->error_code = {};
                payload.complete();
            }
        } while (retry);
    }

    if (failed) {
        // The listener failed: every wait behind the front fails with it.
        std::error_code failure{errno, std::system_category()};
        while (auto* submission = payload.next_submission()) {
            submission->status = Net::OperationStatus::kError;
            submission->error_code = failure;
            payload.complete();
        }
    }
}

static void do_read_file(FileReadChannel* channel) {
    // A file is always ready, so the batch the channel hands over here is the
    // operation: the reads cover consecutive stretches of it.
    auto& payload = channel->submit();
    if (payload.size() == 0) {
        return;  // nothing to read for
    }

    auto& vectors = payload.header();
    ssize_t result = 0;
    do {
        result = ::preadv(channel->native_handle(), vectors.data(), static_cast<int>(vectors.size()),
                          static_cast<off_t>(payload.offset()));
    } while (result < 0 && errno == EINTR);

    if (result < 0) {
        const int error = errno;
        const std::error_code failure{error, std::system_category()};
        while (auto* submission = payload.next_submission()) {
            submission->status = FS::OperationStatus::kError;
            submission->error_code = failure;
            payload.complete();
        }
        return;
    }

    payload.advance_offset(static_cast<std::size_t>(result));
    std::size_t remaining = static_cast<std::size_t>(result);
    while (auto* submission = payload.next_submission()) {
        if (remaining == 0) {
            // A short read on a regular file is its end: the reads behind it would
            // read at or past where it stopped.
            submission->status = FS::OperationStatus::kDone;
            submission->bytes = 0;
            submission->error_code = {};
            payload.complete();
            continue;
        }
        const std::size_t taken = std::min(remaining, submission->buffer.size());
        submission->status = FS::OperationStatus::kDone;
        submission->bytes = taken;
        submission->error_code = {};
        payload.complete();
        remaining -= taken;
    }
}

static void do_write_file(FileWriteChannel* channel) {
    // As for reading: the batch is the operation, and the writes follow one another
    // in the file.
    auto& payload = channel->submit();
    if (payload.size() == 0) {
        return;  // nothing to write
    }

    auto& vectors = payload.header();
    ssize_t result = 0;
    do {
        result = ::pwritev(channel->native_handle(), vectors.data(), static_cast<int>(vectors.size()),
                           static_cast<off_t>(payload.offset()));
    } while (result < 0 && errno == EINTR);

    if (result < 0) {
        const int error = errno;
        const std::error_code failure{error, std::system_category()};
        while (auto* submission = payload.next_submission()) {
            submission->status = FS::OperationStatus::kError;
            submission->error_code = failure;
            payload.complete();
        }
        return;
    }

    payload.advance_offset(static_cast<std::size_t>(result));
    std::size_t remaining = static_cast<std::size_t>(result);
    while (remaining > 0) {
        auto* submission = payload.next_submission();
        if (submission == nullptr) {
            break;
        }
        const std::size_t size = submission->buffer.size();
        const std::size_t taken = std::min(remaining, size);
        submission->bytes += taken;
        remaining -= taken;

        if (taken == size) {
            submission->status = FS::OperationStatus::kDone;
            submission->error_code = {};
            payload.complete();
        } else {
            // The operation stopped inside this write: what is left of it keeps its
            // place in the file.
            submission->buffer = submission->buffer.subspan(taken);
            break;
        }
    }
}

}  // namespace NBIO::Core
#endif  // defined(__linux__)


