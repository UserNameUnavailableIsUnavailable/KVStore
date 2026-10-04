#if defined(__linux__)

#include "URingMultiplexer.hpp"

#include <liburing.h>
#include <poll.h>
#include <sys/socket.h>

#include <Foundation/Core/TcpSocket.hpp>
#include <Foundation/NBIO/Types.hpp>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <variant>

#include "Channel.hpp"
#include "EventNotifyChannel.hpp"
#include "FileReadChannel.hpp"
#include "FileStream.hpp"
#include "FileWriteChannel.hpp"
#include "RdmaAcceptChannel.hpp"
#include "RdmaConnectChannel.hpp"
#include "RdmaReceiveChannel.hpp"
#include "RdmaSendChannel.hpp"
#include "SystemSignalChannel.hpp"
#include "SystemTimerChannel.hpp"
#include "TcpAcceptChannel.hpp"
#include "TcpConnectChannel.hpp"
#include "TcpReceiveChannel.hpp"
#include "TcpSendChannel.hpp"

#define MAKE_ERROR_CODE(e) std::error_code(e, std::system_category())

namespace Foundation::NBIO {
#define ThrowUringError(error, what)                       \
    throw std::system_error(error, std::system_category(), \
                            std::string(what) + ": " + std::system_category().message(error));

using ChannelVariant =
    std::variant<TcpReceiveChannel*, TcpSendChannel*, FileReadChannel*, FileWriteChannel*, TcpAcceptChannel*,
                 SystemTimerChannel*, EventNotifyChannel*, SystemSignalChannel*, RdmaAcceptChannel*,
                 RdmaConnectChannel*, TcpConnectChannel*, RdmaSendChannel*, RdmaReceiveChannel*>;

static ChannelVariant as_variant(ChannelBase* channel) {
    switch (channel->type()) {
        case ChannelType::kReceive:
            return static_cast<TcpReceiveChannel*>(channel);
        case ChannelType::kSend:
            return static_cast<TcpSendChannel*>(channel);
        case ChannelType::kRead:
            return static_cast<FileReadChannel*>(channel);
        case ChannelType::kWrite:
            return static_cast<FileWriteChannel*>(channel);
        case ChannelType::kAccept:
            return static_cast<TcpAcceptChannel*>(channel);
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
    throw std::logic_error("URingMultiplexer::as_variant: unsupported channel type");
}

template <typename Visitor>
static decltype(auto) visit_channel(ChannelBase* channel, Visitor&& visitor) {
    return std::visit(std::forward<Visitor>(visitor), as_variant(channel));
}

void URingMultiplexer::run() { run_impl(-1); }

void URingMultiplexer::run_for(std::chrono::milliseconds timeout) {
    auto now = std::chrono::steady_clock::now();
    const auto due = now + timeout;
    do {
        const auto remaining = (due - now).count();
        const auto ms = remaining > INT_MAX ? INT_MAX : remaining;
        run_impl(static_cast<int>(ms));
        now = std::chrono::steady_clock::now();
    } while (now < due);
}

void URingMultiplexer::run_impl(int timeout_ms) {
    // 1. Give the kernel every operation that is waiting to start.
    submit();
    // 2. wait for at least one completion (unless asked not to block).
    io_uring_cqe* cqe = nullptr;
    int ret = 0;
    if (timeout_ms < 0) {
        do {
            ret = ::io_uring_wait_cqe(&ring_, &cqe);
        } while (ret == -EINTR || ret == EINTR);
    } else {
        __kernel_timespec ts{.tv_sec = static_cast<long long>(timeout_ms / 1000),
                             .tv_nsec = static_cast<long long>(timeout_ms % 1000) * 1000000LL};
        do {
            ret = ::io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
        } while (ret == -EINTR || ret == EINTR);
    }
    // -ETIME simply means "nothing completed" for the timeout variant.
    if (ret < 0 && ret != -ETIME) {
        ThrowUringError(-ret, "io_uring_wait_cqe failed");
    }

    // 3. Reap everything that is ready. A resumed coroutine may arm the next
    //    operation, so submit again to keep latency down.
    handle_completions();
    submit();
}

void URingMultiplexer::add_channel(Foundation::NBIO::ChannelBase* channel) {
    // Registration is the whole of arming: the channel is asked for work until it
    // disarms.
    channels_.insert(channel);
}

void URingMultiplexer::delete_channel(Foundation::NBIO::ChannelBase* channel) noexcept {
    // TODO: cancel or drain the channel's in-flight operation before its frame
    // goes away; until then a completion naming a deleted channel is dropped.
    channels_.erase(channel);
    in_flight_.erase(channel);
}

// One completion's outcome into the batch it belongs to: the result is spread over
// the submissions the operation covered, in queue order.
template <typename T>
static void advance_channel(T* channel, int result) {
    using ChannelType = std::remove_pointer_t<T>;

    if constexpr (std::is_same_v<ChannelType, TcpReceiveChannel>) {
        auto& payload = channel->submit();
        if (result < 0) {
            if (result == -EAGAIN || result == -EWOULDBLOCK) {
                return;
            }
            const std::error_code failure{-result, std::system_category()};
            while (auto* submission = payload.next_submission()) {
                submission->status = Core::OperationStatus::kError;
                submission->error_code = failure;
                payload.complete();
            }
        } else if (result == 0) {
            while (auto* submission = payload.next_submission()) {
                submission->status = Core::OperationStatus::kDone;
                submission->bytes = 0;
                submission->error_code = {};
                payload.complete();
            }
        } else {
            std::size_t remaining = static_cast<std::size_t>(result);
            while (remaining > 0) {
                auto* submission = payload.next_submission();
                if (submission == nullptr) {
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
    } else if constexpr (std::is_same_v<ChannelType, TcpSendChannel>) {
        auto& payload = channel->submit();
        if (result < 0) {
            if (result == -EAGAIN || result == -EWOULDBLOCK) {
                return;
            }
            const std::error_code failure{-result, std::system_category()};
            while (auto* submission = payload.next_submission()) {
                submission->status = Core::OperationStatus::kError;
                submission->error_code = failure;
                payload.complete();
            }
        } else if (result == 0) {
            auto* submission = payload.next_submission();
            if (submission != nullptr && !submission->buffer.empty()) {
                submission->status = Core::OperationStatus::kError;
                submission->error_code = std::make_error_code(std::errc::io_error);
                payload.complete();
            }
        } else {
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
                    submission->status = Core::OperationStatus::kDone;
                    submission->error_code = {};
                    payload.complete();
                } else {
                    submission->buffer = submission->buffer.subspan(taken);
                    break;
                }
            }
        }
    } else if constexpr (std::is_same_v<ChannelType, FileReadChannel>) {
        auto& payload = channel->submit();
        if (result < 0) {
            const std::error_code failure{-result, std::system_category()};
            while (auto* submission = payload.next_submission()) {
                submission->status = Core::OperationStatus::kError;
                submission->error_code = failure;
                payload.complete();
            }
        } else {
            payload.advance_offset(static_cast<std::size_t>(result));
            std::size_t remaining = static_cast<std::size_t>(result);
            while (auto* submission = payload.next_submission()) {
                if (remaining == 0) {
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
    } else if constexpr (std::is_same_v<ChannelType, FileWriteChannel>) {
        auto& payload = channel->submit();
        if (result < 0) {
            const std::error_code failure{-result, std::system_category()};
            while (auto* submission = payload.next_submission()) {
                submission->status = Core::OperationStatus::kError;
                submission->error_code = failure;
                payload.complete();
            }
        } else {
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
                    submission->status = Core::OperationStatus::kDone;
                    submission->error_code = {};
                    payload.complete();
                } else {
                    submission->buffer = submission->buffer.subspan(taken);
                    break;
                }
            }
        }
    } else if constexpr (std::is_same_v<ChannelType, TcpAcceptChannel>) {
        auto& payload = channel->submit();
        auto* submission = payload.next_submission();
        if (submission == nullptr) {
            return;
        }
        if (result >= 0) {
            submission->status = Core::OperationStatus::kDone;
            submission->socket = Core::TcpSocket::adopt(static_cast<std::uintptr_t>(result));
            submission->error_code = {};
            payload.complete();
        } else if (result == -EAGAIN || result == -EWOULDBLOCK) {
            return;
        } else {
            submission->status = Core::OperationStatus::kError;
            submission->error_code = std::error_code(-result, std::system_category());
            payload.complete();
        }
    }
}

template <typename T>
static void complete_channel(T* channel) {
    channel->complete();
}

template <typename T, typename PollPayloadType>
static io_uring_sqe* prepare_poll_sqe(io_uring* ring, T* channel, PollPayloadType& payload, int mask = POLLIN) {
    if (!payload.wants_poll()) {
        return nullptr;
    }
    io_uring_sqe* sqe = ::io_uring_get_sqe(ring);
    if (sqe == nullptr) {
        return nullptr;
    }
    payload.take_poll();
    ::io_uring_prep_poll_add(sqe, channel->native_handle(), mask);
    return sqe;
}

template <typename T>
static io_uring_sqe* prepare_channel(io_uring* ring, T* channel) {
    using ChannelType = std::remove_pointer_t<T>;

    if constexpr (std::is_same_v<ChannelType, TcpReceiveChannel>) {
        auto& payload = channel->submit();
        if (payload.size() == 0) {
            return nullptr;
        }
        auto& message = payload.header();
        io_uring_sqe* sqe = ::io_uring_get_sqe(ring);
        if (sqe == nullptr) {
            return nullptr;
        }
        ::io_uring_prep_recvmsg(sqe, channel->native_handle(), &message, 0);
        return sqe;
    } else if constexpr (std::is_same_v<ChannelType, TcpSendChannel>) {
        auto& payload = channel->submit();
        if (payload.size() == 0) {
            return nullptr;
        }
        auto& message = payload.header();
        io_uring_sqe* sqe = ::io_uring_get_sqe(ring);
        if (sqe == nullptr) {
            return nullptr;
        }
        ::io_uring_prep_sendmsg(sqe, channel->native_handle(), &message, MSG_NOSIGNAL);
        return sqe;
    } else if constexpr (std::is_same_v<ChannelType, FileReadChannel>) {
        auto& payload = channel->submit();
        if (payload.size() == 0) {
            return nullptr;
        }
        auto& vectors = payload.header();
        io_uring_sqe* sqe = ::io_uring_get_sqe(ring);
        if (sqe == nullptr) {
            return nullptr;
        }
        ::io_uring_prep_readv(sqe, channel->native_handle(), vectors.data(), static_cast<unsigned>(vectors.size()),
                              static_cast<__u64>(payload.offset()));
        return sqe;
    } else if constexpr (std::is_same_v<ChannelType, FileWriteChannel>) {
        auto& payload = channel->submit();
        if (payload.size() == 0) {
            return nullptr;
        }
        auto& vectors = payload.header();
        io_uring_sqe* sqe = ::io_uring_get_sqe(ring);
        if (sqe == nullptr) {
            return nullptr;
        }
        ::io_uring_prep_writev(sqe, channel->native_handle(), vectors.data(), static_cast<unsigned>(vectors.size()),
                               static_cast<__u64>(payload.offset()));
        return sqe;
    } else if constexpr (std::is_same_v<ChannelType, TcpAcceptChannel>) {
        auto& payload = channel->submit();
        if (payload.size() == 0) {
            return nullptr;
        }
        payload.bundle();
        auto* submission = payload.next_submission();
        if (submission == nullptr) {
            return nullptr;
        }
        io_uring_sqe* sqe = ::io_uring_get_sqe(ring);
        if (sqe == nullptr) {
            return nullptr;
        }
        ::io_uring_prep_accept(sqe, channel->native_handle(), submission->address.storage(),
                               &submission->address.length(), 0);
        return sqe;
    } else if constexpr (std::is_same_v<ChannelType, SystemTimerChannel>) {
        auto& payload = channel->submit();
        return prepare_poll_sqe(ring, channel, payload);
    } else if constexpr (std::is_same_v<ChannelType, EventNotifyChannel>) {
        auto& payload = channel->submit();
        return prepare_poll_sqe(ring, channel, payload);
    } else if constexpr (std::is_same_v<ChannelType, SystemSignalChannel>) {
        auto& payload = channel->submit();
        return prepare_poll_sqe(ring, channel, payload);
    } else if constexpr (std::is_same_v<ChannelType, RdmaAcceptChannel>) {
        auto& payload = channel->submit();
        return prepare_poll_sqe(ring, channel, payload);
    } else if constexpr (std::is_same_v<ChannelType, RdmaConnectChannel>) {
        auto& payload = channel->submit();
        return prepare_poll_sqe(ring, channel, payload);
    } else if constexpr (std::is_same_v<ChannelType, TcpConnectChannel>) {
        auto& payload = channel->submit();
        return prepare_poll_sqe(ring, channel, payload, POLLOUT);
    } else if constexpr (std::is_same_v<ChannelType, RdmaSendChannel>) {
        auto& payload = channel->submit();
        return prepare_poll_sqe(ring, channel, payload);
    } else if constexpr (std::is_same_v<ChannelType, RdmaReceiveChannel>) {
        auto& payload = channel->submit();
        return prepare_poll_sqe(ring, channel, payload);
    } else {
        return nullptr;
    }
}

URingMultiplexer::URingMultiplexer(std::uint32_t submission_capacity, std::uint32_t completion_capacity)
    : Multiplexer(MultiplexerType::kURing) {
    // Zero-initialised: only the fields we set may influence setup.
    io_uring_params parameters{};
    parameters.flags = IORING_SETUP_CQSIZE;
    parameters.cq_entries = completion_capacity;

    const int ret = ::io_uring_queue_init_params(submission_capacity, &ring_, &parameters);
    if (ret < 0) {
        const int error = -ret;
        if (error == EPERM) {
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

URingMultiplexer::~URingMultiplexer() noexcept { ::io_uring_queue_exit(&ring_); }

bool URingMultiplexer::prepare(Foundation::NBIO::ChannelBase* channel) {
    // One operation per channel is with the kernel at a time.
    if (in_flight_.contains(channel)) {
        return false;
    }

    io_uring_sqe* sqe = visit_channel(channel, [&](auto* typed) { return prepare_channel(&ring_, typed); });

    if (sqe == nullptr) {
        return false;
    }

    // The channel identifies itself; its type says which operation completed.
    ::io_uring_sqe_set_data(sqe, channel);
    in_flight_.insert(channel);
    return true;
}

void URingMultiplexer::submit() {
    bool handed_over = false;
    for (Foundation::NBIO::ChannelBase* channel : channels_) {
        handed_over = prepare(channel) || handed_over;
    }

    if (handed_over) {
        const int ret = ::io_uring_submit(&ring_);
        if (ret < 0) {
            ThrowUringError(-ret, "io_uring_submit failed");
        }
    }
}

void URingMultiplexer::handle_completions() {
    // A channel can have several completions in one pass -- an accept covers one
    // wait, and one wait is one operation -- so every completion is advanced first
    // and only then is each channel asked to reap what it answered. A resumed
    // coroutine is free to prepare more work, and that must not happen while a
    // completion is still being written into the batch it is about to join.
    std::vector<Foundation::NBIO::ChannelBase*> answered;
    answered.reserve(8);

    io_uring_cqe* cqe = nullptr;
    while (::io_uring_peek_cqe(&ring_, &cqe) == 0 && cqe != nullptr) {
        auto* channel = static_cast<Foundation::NBIO::ChannelBase*>(::io_uring_cqe_get_data(cqe));
        if (channel != nullptr) {
            in_flight_.erase(channel);
            // A channel deleted while its operation was in flight is dropped: its
            // frame may already be gone.
            if (channels_.contains(channel)) {
                visit_channel(channel, [&](auto* typed) { advance_channel(typed, cqe->res); });
                if (std::find(answered.begin(), answered.end(), channel) == answered.end()) {
                    answered.push_back(channel);
                }
            }
        }
        ::io_uring_cqe_seen(&ring_, cqe);
    }

    for (Foundation::NBIO::ChannelBase* channel : answered) {
        visit_channel(channel, [](auto* typed) { complete_channel(typed); });
    }
}

}  // namespace Foundation::NBIO
#endif  // defined(__linux__)
