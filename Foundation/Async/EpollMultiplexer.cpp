#include "EpollMultiplexer.hpp"

#include <Foundation/Async/NotifyChannel.hpp>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/unistd.h>

#include <cerrno>
#include <chrono>
#include <climits>
#include <stdexcept>

#include "Channel.hpp"
#include "ListenChannel.hpp"
#include "Multiplexer.hpp"
#include "ReceiveChannel.hpp"
#include "SendChannel.hpp"
#include "TimerChannel.hpp"
#include "spdlog/spdlog.h"

namespace Foundation::Async
{
static void do_receive_data(Channel *base);
static void do_send_data(Channel *base);
static void do_accept_connection(Channel *base);
static void do_wait_timer(Channel *base);
static void do_wait_notifier(Channel* base);

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
    case ChannelType::kListen:
    case ChannelType::kTimer:
    case ChannelType::kNotify:
    case ChannelType::kSignal:
        return EPOLLIN;
    case ChannelType::kSend:
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

    auto fd = channel->get_native_handle();
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
    auto fd = channel->get_native_handle();
    if (registered_channels_.find(fd) == registered_channels_.end())
    {
        throw std::logic_error("channel not added");
    }
    sync_registration(fd, true);
}

void EpollMultiplexer::delete_channel(Channel *channel) noexcept
{
    auto fd = channel->get_native_handle();
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

} // namespace Foundation::Async
