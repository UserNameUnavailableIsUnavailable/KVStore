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

#include "NotifyChannel.hpp"
#include "SignalChannel.hpp"
#include "Types.hpp"
#include "Channel.hpp"
#include "Multiplexer.hpp"
#include "FileStream.hpp"
#include "AcceptChannel.hpp"
#include "ReadChannel.hpp"
#include "ReceiveChannel.hpp"
#include "SendChannel.hpp"
#include "TimerChannel.hpp"
#include "WriteChannel.hpp"
#include "RDMA_ConnectChannel.hpp"
#include "RDMA_AcceptChannel.hpp"
#include "RDMA_SendChannel.hpp"
#include "RDMA_ReceiveChannel.hpp"

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
    case Foundation::NBIO::ChannelType::kTimer:
    case Foundation::NBIO::ChannelType::kNotify:
    case Foundation::NBIO::ChannelType::kRDMA_Accept:
    case Foundation::NBIO::ChannelType::kRDMA_Connect:
    // A stream's completion channel becomes readable when a work completion
    // lands; both simplex halves watch that same fd.
    case Foundation::NBIO::ChannelType::kRDMA_Send:
    case Foundation::NBIO::ChannelType::kRDMA_Receive:
    case Foundation::NBIO::ChannelType::kSignal:
        return EPOLLIN;
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
	for (const auto& it : updated_flags_)
	{
		auto [fd, flags] = it;
		::epoll_event ev{
			.events = flags,
			.data{
				.fd = fd
			}
		};
		spdlog::debug("[EpollMultiplexer::run_impl] fd=0x{:X} flags updated to {}", fd, flags);
		if (epoll_ctl(handle_, EPOLL_CTL_MOD, fd, &ev) != 0)
		{
			throw std::system_error(errno, std::system_category(), "epoll_ctl(MOD) failed");
		}
	}
	updated_flags_.clear();

    // some channels are always ready: ReadChannel; WriteChannel;
	// always-ready channels are added to active_channels_ in update_channel()
    if (!active_channels_.empty())
    {
		timeout_ms = 0; // do not block, poll once to collect active channels and return immediately
    }

    std::array<::epoll_event, 4096> events;
    bool retry = false;
    do
    {
        retry = false;
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

                const auto flags = native_flags_for(channel->type());
                if ((flags & ev) != 0)
                {
                    active_channels_.insert(channel);
                }
            }
        }

        // Take the batch before touching it: disarm() erases an always-ready
        // channel from active_channels_, and handle_completion() may arm one
        // again. Mutating the set while iterating it invalidates the iterator -- the
        // increment then walks a freed node. Any channel re-armed here lands in
        // the now-empty active_channels_ and is picked up on the next pass,
        // which does not block because the set is non-empty again.
        std::unordered_set<Foundation::NBIO::Channel *> batch;
        batch.swap(active_channels_);

        for (auto *channel : batch)
        {
            // The backend does what only it can do -- the I/O itself -- and the
            // channel turns what comes back into the outcome of the waits it
            // covers. Each case then wakes what has an answer; a channel with more
            // to do arms itself again. The channel is named by its concrete type
            // because the submission protocol is the channel's own, not the base
            // class's.
            switch (channel->type())
            {
            case Foundation::NBIO::ChannelType::kReceive:
                do_receive_data(channel);
                static_cast<ReceiveChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kSend:
                do_send_data(channel);
                static_cast<SendChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kAccept:
                do_accept_connection(channel);
                static_cast<AcceptChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kRead:
                do_read_file(channel);
                static_cast<ReadChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kWrite:
                do_write_file(channel);
                static_cast<WriteChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kTimer:
                do_wait_timer(channel);
                static_cast<TimerChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kNotify:
                do_wait_notifier(channel);
                static_cast<NotifyChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kSignal:
                do_drain_signal(channel);
                static_cast<SignalChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kRDMA_Accept:
                static_cast<RDMA_AcceptChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kRDMA_Connect:
                static_cast<RDMA_ConnectChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kRDMA_Send:
                static_cast<RDMA_SendChannel *>(channel)->handle_completion();
                break;
            case Foundation::NBIO::ChannelType::kRDMA_Receive:
                static_cast<RDMA_ReceiveChannel *>(channel)->handle_completion();
                break;
            }
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

    int fd = static_cast<int>(channel->native_handle());
    switch (channel->type())
    {
    case Foundation::NBIO::ChannelType::kReceive:
    case Foundation::NBIO::ChannelType::kSend:
    case Foundation::NBIO::ChannelType::kAccept:
    case Foundation::NBIO::ChannelType::kTimer:
    case Foundation::NBIO::ChannelType::kSignal:
    case Foundation::NBIO::ChannelType::kNotify:
    case Foundation::NBIO::ChannelType::kRead:
    case Foundation::NBIO::ChannelType::kWrite:
    // The RDMA channels are only polled: each one reaps its own completions,
    // because the event says "something finished", not which direction or how much.
    case Foundation::NBIO::ChannelType::kRDMA_Accept:
    case Foundation::NBIO::ChannelType::kRDMA_Connect:
    case Foundation::NBIO::ChannelType::kRDMA_Send:
    case Foundation::NBIO::ChannelType::kRDMA_Receive:
        break;
    default:
        throw std::logic_error("EpollMultiplexer::add_channel: unsupported channel type");
    }

    if (is_always_ready(channel->type()))
    {
		if (always_channels_.contains(fd))
		{
			auto it = always_channels_.find(fd);
			for (; it != always_channels_.end(); it++)
			{
				auto [fd, chan] = *it;
				if (chan == channel) [[unlikely]]
				{
					throw std::logic_error("EpollMultiplexer::add_channel: channel already added");
				}
			}
		}
		always_channels_.emplace(fd, channel);
		return;
    }

	if (pollable_channels_.contains(fd))
	{
		auto it = pollable_channels_.find(fd);
		for (; it != pollable_channels_.end(); it++)
		{
			auto [fd, chan] = *it;
			if (chan == channel) [[unlikely]]
			{
				throw std::logic_error("EpollMultiplexer::add_channel: channel already added");
			}
		}
	}
	else
	{
		epoll_event event{};
		event.events = 0;
		event.data.fd = fd;
		if (epoll_ctl(handle_, EPOLL_CTL_ADD, fd, &event) != 0)
		{
			throw std::system_error(errno, std::system_category(), "epoll_ctl(ADD) failed");
		}
	}
    pollable_channels_.emplace(fd, channel);
}

void EpollMultiplexer::update_channel(Foundation::NBIO::Channel *channel)
{
    const int fd = channel->native_handle();
	const auto type = channel->type();
    if (is_always_ready(type))
    {
        if (channel->armed())
        {
			active_channels_.insert(channel);
        }
        else if (active_channels_.contains(channel))
        {
			active_channels_.erase(channel);
        }
        return;
    }

	auto [begin, end] = pollable_channels_.equal_range(fd);
    if (begin == end)
    {
        throw std::logic_error("channel not added");
    }

	std::uint32_t flags{ 0 };
	for (auto it = begin; it != end; ++it)
	{
		auto *chan = it->second;
		if (chan->armed())
		{
			flags |= native_flags_for(chan->type());
		}
	}
	
	spdlog::debug("[EpollMultiplexer::update_channel] fd=0x{:X} flags updated to {}", fd, flags);
	updated_flags_[fd] = flags;
}

void EpollMultiplexer::delete_channel(Foundation::NBIO::Channel *channel) noexcept
{
	auto fd = channel->native_handle();
	auto type = channel->type();
	active_channels_.erase(channel);

	if (is_always_ready(type))
	{
		auto [begin, end] = always_channels_.equal_range(fd);
		for (auto it = begin; it != end; ++it)
		{
			if (it->second == channel)
			{
				always_channels_.erase(it); // each channel is unique
				break;
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
			return;
		}
		pollable_channels_.erase(target);

	}
	{
		auto [begin, end] = pollable_channels_.equal_range(fd);
		if (begin == end)
		{
			epoll_ctl(handle_, EPOLL_CTL_DEL, fd, nullptr);
			updated_flags_.erase(fd);
			spdlog::debug("[EpollMultiplexer::delete_channel] fd=0x{:X} deleted", fd);
			return;
		}
		std::uint32_t flags{ 0 };
		for (auto it = begin; it != end; ++it)
		{
			auto *chan = it->second;
			if (chan->armed())
			{
				flags |= native_flags_for(chan->type());
			}
		}
		updated_flags_[fd] = flags;
		spdlog::debug("[EpollMultiplexer::delete_channel] fd=0x{:X} flags will update to {}", fd, flags);
	}
}

EpollMultiplexer::~EpollMultiplexer() noexcept
{
    close(handle_);
}

static void do_receive_data(Foundation::NBIO::Channel *base)
{
    // The readiness of the socket is the submission: the channel handed its
    // prepared receives over already, so this only has to drive the operation.
    auto *channel = static_cast<ReceiveChannel *>(base);
    if (!channel->has_submitted())
    {
        return;
    }

    ::msghdr &message = channel->message_batch();
    ssize_t result = 0;
    do
    {
        result = ::recvmsg(channel->native_handle(), &message, 0);
    } while (result < 0 && errno == EINTR);

    channel->complete_tasks(result < 0 ? -errno : result);
}

static void do_send_data(Foundation::NBIO::Channel *base)
{
    // As for receiving: the channel handed its prepared sends over already.
    auto *channel = static_cast<SendChannel *>(base);
    if (!channel->has_submitted())
    {
        return;
    }

    ::msghdr &message = channel->message_batch();
    ssize_t result = 0;
    do
    {
        result = ::sendmsg(channel->native_handle(), &message, MSG_NOSIGNAL);
    } while (result < 0 && errno == EINTR);

    channel->complete_tasks(result < 0 ? -errno : result);
}

static void do_accept_connection(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<AcceptChannel *>(base);

    // One readiness event can mean several connections: take them while they last
    // and while somebody is waiting for one.
    while (true)
    {
        if (channel->submitted_front() == nullptr)
        {
            // The channel hands exactly one wait over at a time, because the
            // answer names the channel and not the wait it belongs to.
            if (channel->count_prepared() == 0 || channel->submitted_front() == nullptr)
            {
                break;
            }
        }

        Foundation::Core::AcceptResult accepted = channel->socket().accept();
        if (accepted.status == Foundation::Core::AcceptStatus::kPending)
        {
            break;
        }

        channel->commit_result(std::move(accepted));
    }
}

void do_wait_timer(Foundation::NBIO::Channel *base)
{
    // The read drains the timerfd; handle_completion() then pops the entries that
    // have come due.
    static_cast<TimerChannel *>(base)->timer().wait();
}

void do_wait_notifier(Foundation::NBIO::Channel *base)
{
    static_cast<NotifyChannel *>(base)->notifier().wait();
}

void do_drain_signal(Foundation::NBIO::Channel *base)
{
    static_cast<SignalChannel *>(base)->signal().drain();
}

static void do_read_file(Foundation::NBIO::Channel *base)
{
    // A file is always ready, so the channel handed its prepared reads over the
    // moment they were awaited; this drives the operation they describe.
    auto *channel = static_cast<ReadChannel *>(base);
    if (!channel->has_submitted())
    {
        return;
    }

    const auto vectors = channel->vectors();
    ssize_t result = 0;
    do
    {
        result = ::preadv(channel->native_handle(), vectors.data(), static_cast<int>(vectors.size()),
                          static_cast<off_t>(channel->front_offset()));
    } while (result < 0 && errno == EINTR);

    channel->complete_tasks(result < 0 ? -errno : result);
}

static void do_write_file(Foundation::NBIO::Channel *base)
{
    // As for reading: the channel owns what a batch of writes looks like. A file
    // is always ready, so the operation it describes is driven here.
    auto *channel = static_cast<WriteChannel *>(base);
    if (!channel->has_submitted())
    {
        return;
    }

    const auto vectors = channel->vectors();
    ssize_t result = 0;
    do
    {
        result = ::pwritev(channel->native_handle(), vectors.data(), static_cast<int>(vectors.size()),
                           static_cast<off_t>(channel->front_offset()));
    } while (result < 0 && errno == EINTR);

    channel->complete_tasks(result < 0 ? -errno : result);
}

} // namespace Foundation::NBIO
#endif // defined(__linux__)
