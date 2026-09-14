#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <chrono>
#include <set>
#include <unordered_map>

#include <Foundation/NBIO/Types.hpp>

#if not defined(__linux__)
#error "EpollMultiplexer is only supported on Linux"
#endif

#include <sys/epoll.h>

namespace Foundation::NBIO
{
class EpollMultiplexer final : public Foundation::NBIO::Multiplexer
{
  public:
    using Handle = int;
    EpollMultiplexer();
    virtual ~EpollMultiplexer() noexcept;
    virtual void run() override;
    virtual void run_for(std::chrono::milliseconds timeout) override;
    virtual void add_channel(Foundation::NBIO::Channel *channel) override;
    virtual void update_channel(Foundation::NBIO::Channel *channel) override;
    virtual void delete_channel(Foundation::NBIO::Channel *channel) noexcept override;
    Handle native_handle() const noexcept
    {
        return handle_;
    }

  private:

    void run_impl(int timeout);
    int handle_{-1}; // epoll file descriptor
    std::unordered_multimap<int, Foundation::NBIO::Channel *> pollable_channels_; // all pollable channels
    std::unordered_multimap<int, Foundation::NBIO::Channel *> always_channels_; // channels that are always ready, non-pollable
	std::unordered_map<int, std::uint32_t> updated_flags_; // delayed update
	std::set<Foundation::NBIO::Channel *> active_channels_;
};
} // namespace Foundation::NBIO
