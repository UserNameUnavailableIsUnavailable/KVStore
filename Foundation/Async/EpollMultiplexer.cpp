#include "EpollMultiplexer.hpp"

#include <Foundation/Async/NotifyChannel.hpp>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/unistd.h>

#include <cerrno>
#include <algorithm>
#include <chrono>
#include <climits>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "Channel.hpp"
#include "FileStream.hpp"
#include "ReadChannel.hpp"
#include "ListenChannel.hpp"
#include "Multiplexer.hpp"
#include "ReceiveChannel.hpp"
#include "SendChannel.hpp"
#include "WriteChannel.hpp"
#include "TimerChannel.hpp"

namespace Foundation::Async
{
static void do_receive_data(Channel *base);
static void do_send_data(Channel *base);
static void do_accept_connection(Channel *base);
static void do_wait_timer(Channel *base);
static void do_wait_notifier(Channel* base);
static void do_file_read(Channel *base);
static void do_file_write(Channel *base);

EpollMultiplexer::EpollMultiplexer()
{
    handle_ = epoll_create1(0);
    if (handle_ < 0)
    {
        throw std::runtime_error("epoll_create1 failed");
    }
}

std::uint32_t EpollMultiplexer::native_flags_for(ChannelType type)
{
    switch (type)
    {
    case ChannelType::kReceive:
    case ChannelType::kRead:
    case ChannelType::kListen:
    case ChannelType::kTimer:
    case ChannelType::kNotify:
    case ChannelType::kSignal:
        return EPOLLIN;
    case ChannelType::kSend:
    case ChannelType::kWrite:
        return EPOLLOUT;
    default:
        throw std::logic_error("EpollMultiplexer::native_flags_for: unsupported channel type");
    }
}

std::uint32_t EpollMultiplexer::combine_flags_for(int fd) const
{
    std::uint32_t flags = 0;
    auto range = registered_channels_.equal_range(fd);
    for (auto it = range.first; it != range.second; ++it)
    {
        const Channel *channel = it->second;
        if (channel != nullptr && channel->armed())
        {
            flags |= native_flags_for(channel->type());
        }
    }
    return flags;
}

void EpollMultiplexer::sync_registration(int fd, bool already_added)
{
    const auto flags = combine_flags_for(fd);

    epoll_event event{};
    event.events = flags;
    event.data.fd = fd;

    const int op = already_added ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    if (epoll_ctl(handle_, op, fd, &event) != 0)
    {
        throw std::system_error(errno, std::system_category(),
                                already_added ? "epoll_ctl(MOD) failed" : "epoll_ctl(ADD) failed");
    }
}

// run once
void EpollMultiplexer::run_impl(int timeout_ms)
{
    if (!always_ready_channels_.empty())
    {
        auto channels = std::move(always_ready_channels_);
        always_ready_channels_.clear();
        for (auto *channel : channels)
        {
            if (channel == nullptr)
            {
                continue;
            }
            channel->disarm();
            if (channel->type() == ChannelType::kRead)
            {
                do_file_read(channel);
            }
            else if (channel->type() == ChannelType::kWrite)
            {
                do_file_write(channel);
            }
            channel->on_event();
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
            channel->on_event();
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

void EpollMultiplexer::add_channel(Channel *channel)
{
    // IMPORTANT: The channel may register itself at any time.
    // If the channel is incomplete (e.g., has not finished construction yet),
    // calling virtual functions results in runtime error!

    auto fd = channel->native_handle();
    if (channel->type() == ChannelType::kRead || channel->type() == ChannelType::kWrite)
    {
        return;
    }
    switch (channel->type())
    {
    default:
        break;
    }
    // Teach the channel how to do its I/O under epoll, based on its type. A
    // type this multiplexer does not support is rejected.
    switch (channel->type())
    {
    case ChannelType::kReceive:
        channel->on_event(&do_receive_data);
        break;
    case ChannelType::kSend:
        channel->on_event(&do_send_data);
        break;
    case ChannelType::kListen:
        channel->on_event(&do_accept_connection);
        break;
    case ChannelType::kTimer:
        channel->on_event(&do_wait_timer);
        break;
    case ChannelType::kSignal:
        // No handler: SignalChannel::OnEvent drains the signalfd itself.
        break;
    case ChannelType::kNotify:
        channel->on_event(&do_wait_notifier);
        break;
    default:
        throw std::logic_error("EpollMultiplexer::add_channel: unsupported channel type");
    }

    // First channel for this fd -> ADD; otherwise fold into the existing entry.
    const bool already_added = registered_channels_.count(fd) != 0;
    registered_channels_.emplace(fd, channel);
    sync_registration(fd, already_added);
}

void EpollMultiplexer::update_channel(Channel *channel)
{
    if (channel->type() == ChannelType::kRead || channel->type() == ChannelType::kWrite)
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
    auto fd = channel->native_handle();
    if (registered_channels_.find(fd) == registered_channels_.end())
    {
        throw std::logic_error("channel not added");
    }
    sync_registration(fd, true);
}

void EpollMultiplexer::delete_channel(Channel *channel) noexcept
{
    if (channel->type() == ChannelType::kRead || channel->type() == ChannelType::kWrite)
    {
        std::erase(always_ready_channels_, channel);
        return;
    }
    auto fd = channel->native_handle();
    auto range = registered_channels_.equal_range(fd);
    for (auto it = range.first; it != range.second; ++it)
    {
        if (it->second == channel)
        {
            registered_channels_.erase(it);
            break;
        }
    }

    if (registered_channels_.count(fd) == 0)
    {
        // Last channel for this fd: drop the epoll registration entirely.
        if (epoll_ctl(handle_, EPOLL_CTL_DEL, fd, nullptr) != 0)
        {
            spdlog::warn("Failed to remove fd from epoll.");
        }
        return;
    }

    // Other channels still live on this fd: keep it registered, with the union
    // of the remaining interests.
    sync_registration(fd, true);
}

EpollMultiplexer::~EpollMultiplexer() noexcept
{
    close(handle_);
}

static void do_receive_data(Channel *base)
{
    auto *channel = static_cast<ReceiveChannel *>(base);
    auto &job = channel->job();
    job.result = channel->socket().receive(job.buffer->appendable_span());
    job.buffer->commit(job.result.bytes_transferred);
}

static void do_send_data(Channel *base)
{
    auto *channel = static_cast<SendChannel *>(base);
    auto &job = channel->job();
    job.result = channel->socket().send(job.buffer->valid_span());
    job.buffer->consume(job.result.bytes_transferred);
}

static void do_accept_connection(Channel *base)
{
    auto *channel = static_cast<ListenChannel *>(base);
    auto &job = channel->job();
    job.result = channel->socket().accept();
}

void do_wait_timer(Channel *base)
{
    auto *channel = static_cast<TimerChannel *>(base);
    channel->timer().wait();
}

void do_wait_notifier(Channel* base)
{
    auto *channel = static_cast<NotifyChannel *>(base);
    channel->notifier().wait();
}

static void do_file_read(Channel *base)
{
    auto *channel = static_cast<ReadChannel *>(base);
    auto &job = channel->job();
    while (true)
    {
        const auto chunk = job.buffer->appendable_span();
        const auto result = ::pread(channel->native_handle(), chunk.data(), chunk.size(),
                                    static_cast<off_t>(job.offset));
        if (result < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            job.result = {.status = ReadStatus::kError, .bytes_transferred = 0, .error_code = std::error_code(errno, std::system_category())};
            return;
        }
        if (result == 0)
        {
            job.result = {.status = ReadStatus::kEndOfFile, .bytes_transferred = 0, .error_code = {}};
            return;
        }

        job.buffer->commit(static_cast<std::size_t>(result));
        job.offset += static_cast<std::uint64_t>(result);
        channel->file().advance_read_offset(static_cast<std::size_t>(result));
        job.result = {.status = ReadStatus::kDone,
                      .bytes_transferred = static_cast<std::size_t>(result),
                      .error_code = {}};
        return;
    }
}

static void do_file_write(Channel *base)
{
    auto *channel = static_cast<WriteChannel *>(base);
    auto &job = channel->job();
    auto total = std::size_t{0};
    while (job.buffer->valid_size() > 0)
    {
        const auto chunk = job.buffer->valid_span();
        const auto result = ::pwrite(channel->native_handle(), chunk.data(), chunk.size(),
                                     static_cast<off_t>(job.offset));
        if (result < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            job.result = {.status = WriteStatus::kError,
                          .bytes_transferred = total,
                          .error_code = std::error_code(errno, std::system_category())};
            return;
        }
        if (result == 0)
        {
            job.result = {.status = WriteStatus::kError,
                          .bytes_transferred = total,
                          .error_code = std::make_error_code(std::errc::io_error)};
            return;
        }

        job.buffer->consume(static_cast<std::size_t>(result));
        job.offset += static_cast<std::uint64_t>(result);
        channel->file().advance_write_offset(static_cast<std::size_t>(result));
        total += static_cast<std::size_t>(result);
    }

    job.result = {.status = WriteStatus::kDone, .bytes_transferred = total, .error_code = {}};
}

} // namespace Foundation::Async
