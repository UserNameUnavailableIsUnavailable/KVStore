#pragma once

#include <Foundation/Core/Result.hpp>

#include <cstddef>
#include <system_error>
#include <variant>
#include <vector>
#include <deque>
#include <cassert>

#if defined(__unix__)
#include <sys/socket.h>
#include <sys/uio.h>

namespace Foundation::NBIO::detail
{
// A batchable channel moves its waits through three queues: prepared (awaited,
// not yet handed over), submitted (the operation is out there), and completed
// (it has an answer). Every payload below is a thin wrapper over that state plus
// whatever the kernel-facing operation needs built (an msghdr, a vector of
// iovecs, a peer address slot).
template <typename C>
class CommunicationPayload
{
public:
    // Move everything prepared into submitted: it is now the current operation.
    void bundle()
    {
        submitted_.insert(submitted_.end(), prepared_.begin(), prepared_.end());
        prepared_.clear();
    }
    // How many waits are still owed an answer.
    std::size_t size() const noexcept
    {
        return prepared_.size() + submitted_.size();
    }
    void submit(Core::Communication* communication)
    {
        assert(communication != nullptr);
        prepared_.push_back(communication);
    }
    Core::Communication* next_submission()
    {
        return submitted_.empty() ? nullptr : submitted_.front();
    }
    // One submission got its answer: move it to the completed queue.
    void complete() noexcept
    {
        assert(!submitted_.empty());
        completed_.push_back(submitted_.front());
        submitted_.pop_front();
    }
    Core::Communication* next_completion()
    {
        return completed_.empty() ? nullptr : completed_.front();
    }
    void conclude() noexcept
    {
        assert(!completed_.empty());
        completed_.pop_front();
    }

private:
    std::deque<Core::Communication*> prepared_{};
    std::deque<Core::Communication*> submitted_{};
    std::deque<Core::Communication*> completed_{};
};

// One recvmsg/sendmsg over every submitted transmission, in queue order. The
// front transmission takes the bytes and a short operation leaves the rest where
// they are; a partial send advances the front transmission's buffer rather than
// completing it, so the remainder is retried next time.
template <typename C>
class MessagePayload
{
public:
    ::msghdr& header() noexcept
    {
        submitted_.insert(submitted_.end(), prepared_.begin(), prepared_.end());
        prepared_.clear();
        vectors_.clear();
        for (auto* trans : submitted_)
        {
            vectors_.emplace_back(::iovec{.iov_base = trans->buffer.data(), .iov_len = trans->buffer.size()});
        }
        header_.msg_iov = vectors_.data();
        header_.msg_iovlen = vectors_.size();
        return header_;
    }
    std::size_t size() const noexcept
    {
        return submitted_.size() + prepared_.size();
    }
    void submit(Core::Transmission* transmission)
    {
        assert(transmission != nullptr);
        prepared_.push_back(transmission);
    }
    Core::Transmission* next_submission()
    {
        return submitted_.empty() ? nullptr : submitted_.front();
    }
    void complete() noexcept
    {
        assert(!submitted_.empty());
        completed_.push_back(submitted_.front());
        submitted_.pop_front();
    }
    Core::Transmission* next_completion()
    {
        return completed_.empty() ? nullptr : completed_.front();
    }
    void conclude() noexcept
    {
        assert(!completed_.empty());
        completed_.pop_front();
    }

private:
    ::msghdr header_{};
    std::vector<::iovec> vectors_;
    std::deque<Core::Transmission*> prepared_{};
    std::deque<Core::Transmission*> submitted_{};
    std::deque<Core::Transmission*> completed_{};
};

// One preadv/pwritev over the submitted transmissions. The whole batch is a
// single vector at one file offset, which the channel seeds and the backend
// advances by what the kernel actually moved.
template <typename C>
class IOVectorPayload
{
public:
    std::vector<::iovec>& header() noexcept
    {
        submitted_.insert(submitted_.end(), prepared_.begin(), prepared_.end());
        prepared_.clear();
        vectors_.clear();
        for (auto* trans : submitted_)
        {
            vectors_.emplace_back(::iovec{.iov_base = trans->buffer.data(), .iov_len = trans->buffer.size()});
        }
        return vectors_;
    }
    // Where the batch starts in the file.
    std::uint64_t offset() const noexcept
    {
        return offset_;
    }
    void set_offset(std::uint64_t offset) noexcept
    {
        offset_ = offset;
    }
    void advance_offset(std::size_t bytes) noexcept
    {
        offset_ += static_cast<std::uint64_t>(bytes);
    }
    std::size_t size() const noexcept
    {
        return submitted_.size() + prepared_.size();
    }
    void submit(Core::Transmission* transmission)
    {
        assert(transmission != nullptr);
        prepared_.push_back(transmission);
    }
    Core::Transmission* next_submission()
    {
        return submitted_.empty() ? nullptr : submitted_.front();
    }
    void complete() noexcept
    {
        assert(!submitted_.empty());
        completed_.push_back(submitted_.front());
        submitted_.pop_front();
    }
    Core::Transmission* next_completion()
    {
        return completed_.empty() ? nullptr : completed_.front();
    }
    void conclude() noexcept
    {
        assert(!completed_.empty());
        completed_.pop_front();
    }

private:
    std::uint64_t offset_{0};
    std::vector<::iovec> vectors_;
    std::deque<Core::Transmission*> prepared_{};
    std::deque<Core::Transmission*> submitted_{};
    std::deque<Core::Transmission*> completed_{};
};

// The channels that only poll carry no data: their whole wait is a one-shot poll
// of a descriptor they drain themselves. The payload is just whether a poll is
// already out there, which is what stops the backend handing over a second one.
template <typename C>
class PollPayload
{
public:
    bool wants_poll() const noexcept
    {
        return !submitted_;
    }
    void take_poll() noexcept
    {
        submitted_ = true;
    }
    void release_poll() noexcept
    {
        submitted_ = false;
    }
    bool outstanding() const noexcept
    {
        return submitted_;
    }

private:
    bool submitted_{false};
};
} // namespace Foundation::NBIO::detail
#endif


namespace Foundation::NBIO
{
class TcpAcceptChannel;
class TcpConnectChannel;
class TcpSendChannel;
class TcpReceiveChannel;
class FileReadChannel;
class FileWriteChannel;
class EventNotifyChannel;
class SystemSignalChannel;
class SystemTimerChannel;
class RdmaAcceptChannel;
class RdmaConnectChannel;
class RdmaSendChannel;
class RdmaReceiveChannel;

using AcceptPayload = detail::CommunicationPayload<TcpAcceptChannel>;
using ConnectPayload = detail::PollPayload<TcpConnectChannel>;
using SendPayload = detail::MessagePayload<TcpSendChannel>;
using ReceivePayload = detail::MessagePayload<TcpReceiveChannel>;
using ReadPayload = detail::IOVectorPayload<FileReadChannel>;
using WritePayload = detail::IOVectorPayload<FileWriteChannel>;
using NotifyPayload = detail::PollPayload<EventNotifyChannel>;
using SystemSignalPayload = detail::PollPayload<SystemSignalChannel>;
using SystemTimerPayload = detail::PollPayload<SystemTimerChannel>;
using RdmaAcceptPayload = detail::PollPayload<RdmaAcceptChannel>;
using RdmaConnectPayload = detail::PollPayload<RdmaConnectChannel>;
using RdmaSendPayload = detail::PollPayload<RdmaSendChannel>;
using RdmaReceivePayload = detail::PollPayload<RdmaReceiveChannel>;

using Payload = std::variant<
    AcceptPayload,
    ConnectPayload,
    SendPayload,
    ReceivePayload,
    ReadPayload,
    WritePayload,
    NotifyPayload,
    SystemSignalPayload,
    SystemTimerPayload,
    RdmaAcceptPayload,
    RdmaConnectPayload,
    RdmaSendPayload,
    RdmaReceivePayload>;
} // namespace Foundation::NBIO