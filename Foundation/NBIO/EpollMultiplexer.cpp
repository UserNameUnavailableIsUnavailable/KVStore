#include "EpollMultiplexer.hpp"

#include <Foundation/NBIO/NotifyChannel.hpp>
#include <Foundation/NBIO/SignalChannel.hpp>
#include <Foundation/NBIO/Types.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "FileStream.hpp"
#include "ListenChannel.hpp"
#include "ReadChannel.hpp"
#include "ReceiveChannel.hpp"
#include "SendChannel.hpp"
#include "TimerChannel.hpp"
#include "WriteChannel.hpp"

namespace Foundation::NBIO
{
static void do_receive_data(Foundation::NBIO::Channel *base);
static void do_send_data(Foundation::NBIO::Channel *base);
static void do_accept_connection(Foundation::NBIO::Channel *base);
static void do_wait_timer(Foundation::NBIO::Channel *base);
static void do_wait_notifier(Foundation::NBIO::Channel *base);
static void do_drain_signal(Foundation::NBIO::Channel *base);
static void do_read_file(Foundation::NBIO::Channel *base);
static void do_write_file(Foundation::NBIO::Channel *base);

EpollMultiplexer::EpollMultiplexer()
{
    handle_ = epoll_create1(0);
    if (handle_ < 0)
    {
        throw std::runtime_error("epoll_create1 failed");
    }
}

std::uint32_t EpollMultiplexer::native_flags_for(Foundation::NBIO::ChannelType type)
{
    switch (type)
    {
    case Foundation::NBIO::ChannelType::kReceive:
    case Foundation::NBIO::ChannelType::kRead:
    case Foundation::NBIO::ChannelType::kListen:
    case Foundation::NBIO::ChannelType::kTimer:
    case Foundation::NBIO::ChannelType::kNotify:
    case Foundation::NBIO::ChannelType::kSignal:
        return EPOLLIN;
    case Foundation::NBIO::ChannelType::kSend:
    case Foundation::NBIO::ChannelType::kWrite:
        return EPOLLOUT;
    default:
        throw std::logic_error("EpollMultiplexer::native_flags_for: unsupported channel type");
    }
}

constexpr std::uint32_t active_flags_for(Foundation::NBIO::ChannelType type)
{
    switch (type)
    {
    case Foundation::NBIO::ChannelType::kReceive:
    case Foundation::NBIO::ChannelType::kRead:
    case Foundation::NBIO::ChannelType::kListen:
    case Foundation::NBIO::ChannelType::kTimer:
    case Foundation::NBIO::ChannelType::kNotify:
    case Foundation::NBIO::ChannelType::kSignal:
        return EPOLLIN;
    case Foundation::NBIO::ChannelType::kSend:
    case Foundation::NBIO::ChannelType::kWrite:
        return EPOLLOUT;
    default:
        throw std::logic_error("EpollMultiplexer::active_flags_for: unsupported channel type");
    }
}

constexpr std::uint32_t inactive_flags_for(Foundation::NBIO::ChannelType type)
{
    switch (type)
    {
    case Foundation::NBIO::ChannelType::kReceive:
    case Foundation::NBIO::ChannelType::kRead:
    case Foundation::NBIO::ChannelType::kListen:
    case Foundation::NBIO::ChannelType::kTimer:
    case Foundation::NBIO::ChannelType::kNotify:
    case Foundation::NBIO::ChannelType::kSignal:
    case Foundation::NBIO::ChannelType::kSend:
    case Foundation::NBIO::ChannelType::kWrite:
        return 0;
    default:
        throw std::logic_error("EpollMultiplexer::inactive_flags_for: unsupported channel type");
    }
}

// run once
void EpollMultiplexer::run_impl(int timeout_ms)
{
    // some channels are always ready: ReadChannel; WriteChannel;
    if (!always_ready_channels_.empty())
    {
        auto channels = std::move(always_ready_channels_);
        always_ready_channels_.clear();
        for (auto *channel : channels)
        {
            if (channel == nullptr) [[unlikely]]
            {
                continue;
            }
            channel->disarm();
            channel->handle_event();
        }
        return;
    }

    epoll_event events[1024];
    bool retry = false;
    do
    {
        retry = false;
        auto n = epoll_wait(handle_, events, 1024, timeout_ms);
        if (n < 0)
        {
            if (errno == EINTR)
            {
                retry = true;
                continue; // retry
            }
            throw std::system_error(errno, std::system_category(), "epoll_wait failed");
        }

        active_channels_.clear();

        for (auto i = 0; i < n; ++i)
        {
            const auto fd = events[i].data.fd;
            const auto ev = events[i].events;

            // epoll reports ERR/HUP regardless of the requested interest, and
            // they do not match EPOLLIN bit-wise -- count them as readable, or
            // a failing connection would never wake its receive channel.
            const bool readable = (ev & (EPOLLIN | EPOLLERR | EPOLLHUP)) != 0;
            const bool writable = (ev & EPOLLOUT) != 0;

            auto range = registered_channels_.equal_range(fd);
            for (auto it = range.first; it != range.second; ++it)
            {
                auto *channel = it->second;
                if (channel == nullptr)
                {
                    continue;
                }
                const auto flag = native_flags_for(channel->type());
                if (((flag == EPOLLIN) && readable) || ((flag == EPOLLOUT) && writable))
                {
                    active_channels_.push_back(channel);
                }
            }
        }

        for (auto *channel : active_channels_)
        {
            channel->disarm();
            channel->handle_event();
        }
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
    // IMPORTANT: The channel may register itself at any time.
    // If the channel is incomplete (e.g., has not finished construction yet),
    // calling virtual functions results in runtime error!

    auto fd = channel->native_handle();
    bool always_ready{ false }; // Is this channel always-ready? E.g., ReadChannel & WriteChannel.

    switch (channel->type())
    {
    case Foundation::NBIO::ChannelType::kReceive:
        channel->on_event(do_receive_data);
        break;
    case Foundation::NBIO::ChannelType::kSend:
        channel->on_event(do_send_data);
        break;
    case Foundation::NBIO::ChannelType::kListen:
        channel->on_event(do_accept_connection);
        break;
    case Foundation::NBIO::ChannelType::kTimer:
        channel->on_event(do_wait_timer);
        break;
    case Foundation::NBIO::ChannelType::kSignal:
        channel->on_event(do_drain_signal);
        break;
    case Foundation::NBIO::ChannelType::kNotify:
        channel->on_event(do_wait_notifier);
        break;
    case Foundation::NBIO::ChannelType::kRead:
        channel->on_event(do_read_file);
        always_ready = true;
        break;
    case Foundation::NBIO::ChannelType::kWrite:
        channel->on_event(do_write_file);
        always_ready = true;
        break;
    default:
        throw std::logic_error("EpollMultiplexer::add_channel: unsupported channel type");
    }
    if (always_ready)
    {
        return;
    }

    // Register the fd with no interest: the channel is disarmed until its first
    // arm(), at which point update_channel() applies the real flags.
    registered_channels_.emplace(fd, channel);

    epoll_event event{};
    event.events = 0;
    event.data.fd = fd;
    if (epoll_ctl(handle_, EPOLL_CTL_ADD, fd, &event) != 0)
    {
        throw std::system_error(errno, std::system_category(), "epoll_ctl(ADD) failed");
    }
}

void EpollMultiplexer::update_channel(Foundation::NBIO::Channel *channel)
{
    if (channel->type() == Foundation::NBIO::ChannelType::kRead ||
        channel->type() == Foundation::NBIO::ChannelType::kWrite)
    {
        if (channel->armed())
        {
            if (std::find(always_ready_channels_.begin(), always_ready_channels_.end(), channel) ==
                always_ready_channels_.end())
            {
                always_ready_channels_.push_back(channel);
            }
        }
        else
        {
            std::erase(always_ready_channels_, channel);
        }
        return;
    }

    const int fd = channel->native_handle();
    if (!registered_channels_.contains(fd))
    {
        throw std::logic_error("channel not added");
    }

    // Armed contributes the channel's interest; disarmed contributes nothing.
    // epoll_ctl(MOD) with events=0 keeps the fd registered but silent.
    const std::uint32_t flags =
        channel->armed() ? active_flags_for(channel->type()) : inactive_flags_for(channel->type());

    epoll_event event{};
    event.events = flags;
    event.data.fd = fd;
    if (epoll_ctl(handle_, EPOLL_CTL_MOD, fd, &event) != 0)
    {
        throw std::system_error(errno, std::system_category(), "epoll_ctl(MOD) failed");
    }
}

void EpollMultiplexer::delete_channel(Foundation::NBIO::Channel *channel) noexcept
{
    if (channel->type() == Foundation::NBIO::ChannelType::kRead ||
        channel->type() == Foundation::NBIO::ChannelType::kWrite)
    {
        std::erase(always_ready_channels_, channel);
        return;
    }

    const int fd = channel->native_handle();
    registered_channels_.erase(fd);
    if (epoll_ctl(handle_, EPOLL_CTL_DEL, fd, nullptr) != 0)
    {
        spdlog::warn("Failed to remove fd from epoll.");
    }
}

EpollMultiplexer::~EpollMultiplexer() noexcept
{
    close(handle_);
}

static void do_receive_data(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<ReceiveChannel *>(base);
    auto &job = channel->job();
    job.result = channel->socket().receive(job.buffer->appendable_span());
    job.buffer->commit(job.result.bytes_transferred);
}

static void do_send_data(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<SendChannel *>(base);
    auto &job = channel->job();
    job.result = channel->socket().send(job.buffer->valid_span());
    job.buffer->consume(job.result.bytes_transferred);
}

static void do_accept_connection(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<ListenChannel *>(base);
    auto &job = channel->job();
    job.result = channel->socket().accept();
}

void do_wait_timer(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<TimerChannel *>(base);
    channel->timer().wait();
}

void do_wait_notifier(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<NotifyChannel *>(base);
    channel->notifier().wait();
}

void do_drain_signal(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<SignalChannel *>(base);
    channel->signal().drain();
}

static void do_read_file(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<ReadChannel *>(base);
    auto &job = channel->job();
    while (true)
    {
        const auto chunk = job.buffer->appendable_span();
        const auto result =
            ::pread(channel->native_handle(), chunk.data(), chunk.size(), static_cast<off_t>(job.offset));
        if (result < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            job.result = {.status = Foundation::Core::ReadStatus::kError,
                          .bytes_transferred = 0,
                          .error_code = std::error_code(errno, std::system_category())};
            return;
        }
        if (result == 0)
        {
            job.result = {.status = Foundation::Core::ReadStatus::kEndOfFile, .bytes_transferred = 0, .error_code = {}};
            return;
        }

        job.buffer->commit(static_cast<std::size_t>(result));
        job.offset += static_cast<std::uint64_t>(result);
        channel->file_stream().advance_read_offset(static_cast<std::size_t>(result));
        job.result = {.status = Foundation::Core::ReadStatus::kDone,
                      .bytes_transferred = static_cast<std::size_t>(result),
                      .error_code = {}};
        return;
    }
}

static void do_write_file(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<WriteChannel *>(base);
    auto &job = channel->job();
    auto total = std::size_t{0};
    while (job.buffer->valid_size() > 0)
    {
        const auto chunk = job.buffer->valid_span();
        const auto result =
            ::pwrite(channel->native_handle(), chunk.data(), chunk.size(), static_cast<off_t>(job.offset));
        if (result < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            job.result = {.status = Foundation::Core::WriteStatus::kError,
                          .bytes_transferred = total,
                          .error_code = std::error_code(errno, std::system_category())};
            return;
        }
        if (result == 0)
        {
            job.result = {.status = Foundation::Core::WriteStatus::kError,
                          .bytes_transferred = total,
                          .error_code = std::make_error_code(std::errc::io_error)};
            return;
        }

        job.buffer->consume(static_cast<std::size_t>(result));
        job.offset += static_cast<std::uint64_t>(result);
        channel->file().advance_write_offset(static_cast<std::size_t>(result));
        total += static_cast<std::size_t>(result);
    }

    job.result = {.status = Foundation::Core::WriteStatus::kDone, .bytes_transferred = total, .error_code = {}};
}

} // namespace Foundation::NBIO
