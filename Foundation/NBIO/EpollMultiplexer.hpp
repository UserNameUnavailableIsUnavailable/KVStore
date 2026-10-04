#pragma once
#if defined(__linux__)

#include <sys/epoll.h>

#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Channel.hpp"
#include "Multiplexer.hpp"

namespace Foundation::NBIO {
class EpollMultiplexer final : public Foundation::NBIO::Multiplexer {
   public:
    using Handle = int;
    EpollMultiplexer();
    virtual ~EpollMultiplexer() noexcept;
    virtual void run() override;
    virtual void run_for(std::chrono::milliseconds timeout) override;
    virtual void add_channel(Foundation::NBIO::ChannelBase* channel) override;
    virtual void delete_channel(Foundation::NBIO::ChannelBase* channel) noexcept override;

   private:
    void run_impl(int timeout);
    int handle_{-1};                                                                  // epoll file descriptor
    std::unordered_multimap<int, Foundation::NBIO::ChannelBase*> pollable_channels_;  // all pollable channels
    std::unordered_multimap<int, Foundation::NBIO::ChannelBase*>
        always_channels_;  // channels that are always ready, non-pollable
    std::vector<ChannelBase*> active_channels_;
};
}  // namespace Foundation::NBIO
#endif  // defined(__linux__)
