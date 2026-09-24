#pragma once
#if defined(__linux__)

#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <chrono>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "Channel.hpp"
#include "Multiplexer.hpp"

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
    virtual void delete_channel(Foundation::NBIO::Channel *channel) noexcept override;

  private:

    void run_impl(int timeout);
    int handle_{-1}; // epoll file descriptor
    std::unordered_multimap<int, Foundation::NBIO::Channel *> pollable_channels_; // all pollable channels
    std::unordered_multimap<int, Foundation::NBIO::Channel *> always_channels_; // channels that are always ready, non-pollable
    std::vector<Channel*> active_channels_;
};
} // namespace Foundation::NBIO
#endif // defined(__linux__)
