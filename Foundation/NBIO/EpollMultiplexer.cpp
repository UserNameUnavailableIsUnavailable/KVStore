#if defined(__linux__)
#include "EpollMultiplexer.hpp"

#include <cassert>
#include <spdlog/spdlog.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/unistd.h>

#include <cerrno>
#include <chrono>
#include <climits>
#include <stdexcept>
#include <system_error>
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
    case Foundation::NBIO::ChannelType::kListen:
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
        // channel from active_channels_, and handle_event() may arm one again.
        // Mutating the set while iterating it invalidates the iterator -- the
        // increment then walks a freed node. Any channel re-armed here lands in
        // the now-empty active_channels_ and is picked up on the next pass,
        // which does not block because the set is non-empty again.
        std::set<Foundation::NBIO::Channel *> batch;
        batch.swap(active_channels_);

        for (auto *channel : batch)
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
	Channel::IOHandler handler{ nullptr };

    switch (channel->type())
    {
    case Foundation::NBIO::ChannelType::kReceive:
        handler = do_receive_data;
        break;
    case Foundation::NBIO::ChannelType::kSend:
        handler = do_send_data;
        break;
    case Foundation::NBIO::ChannelType::kListen:
        handler = do_accept_connection;
        break;
    case Foundation::NBIO::ChannelType::kTimer:
        handler = do_wait_timer;
        break;
    case Foundation::NBIO::ChannelType::kSignal:
        handler = do_drain_signal;
        break;
    case Foundation::NBIO::ChannelType::kNotify:
        handler = do_wait_notifier;
        break;
    // The RDMA channels have no backend handler: each one reaps its own
    // completions in handle_event(), because the event only says "something
    // finished", not which direction or how much.
    case Foundation::NBIO::ChannelType::kRDMA_Accept:
    case Foundation::NBIO::ChannelType::kRDMA_Connect:
    case Foundation::NBIO::ChannelType::kRDMA_Send:
    case Foundation::NBIO::ChannelType::kRDMA_Receive:
        handler = nullptr;
        break;
    case Foundation::NBIO::ChannelType::kRead:
        handler = do_read_file;
        break;
    case Foundation::NBIO::ChannelType::kWrite:
        handler = do_write_file;
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
		channel->on_event(handler);
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
	channel->on_event(handler);
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
    auto *channel = static_cast<ReceiveChannel *>(base);
    auto &job = channel->job();
    job.result = channel->socket().receive(job.buffer);
}

static void do_send_data(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<SendChannel *>(base);
    auto &job = channel->job();
    const auto result = channel->socket().send(job.buffer);
    if (result.status == Foundation::Core::SendStatus::kDone ||
        result.status == Foundation::Core::SendStatus::kPending)
    {
        // Consume what the kernel took: the span is what gets sent again, so a
        // partial send has to resume with the remainder rather than repeat.
        // The caller is told the total, because that is how much of its own
        // buffer it drops.
        job.buffer = job.buffer.subspan(result.bytes_transferred);
        job.result.bytes_transferred += result.bytes_transferred;
        job.result.status = job.buffer.empty() ? Foundation::Core::SendStatus::kDone
                                               : Foundation::Core::SendStatus::kPending;
        job.result.error_code = {};
        return;
    }

    job.result = result;
}

static void do_accept_connection(Foundation::NBIO::Channel *base)
{
    auto *channel = static_cast<AcceptChannel *>(base);
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
        const auto chunk = job.buffer;
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
    while (job.buffer.size() > 0)
    {
        const auto chunk = job.buffer;
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

        job.offset += static_cast<std::uint64_t>(result);
        channel->file().advance_write_offset(static_cast<std::size_t>(result));
        total += static_cast<std::size_t>(result);
        // Consume what was written. The job holds a span, so nothing shrinks it
        // implicitly: without this the loop re-sends the same bytes at
        // ever-increasing offsets and never terminates.
        job.buffer = job.buffer.subspan(static_cast<std::size_t>(result));
    }

    job.result = {.status = Foundation::Core::WriteStatus::kDone, .bytes_transferred = total, .error_code = {}};
}

} // namespace Foundation::NBIO
#endif // defined(__linux__)
