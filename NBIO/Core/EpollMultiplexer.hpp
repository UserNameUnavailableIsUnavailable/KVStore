#pragma once
#if defined(__linux__)

#include <sys/epoll.h>

#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Channel.hpp"
#include "Multiplexer.hpp"

namespace NBIO::Core {
class EpollMultiplexer final : public NBIO::Core::Multiplexer {
   public:
    using Handle = int;
    EpollMultiplexer();
    virtual ~EpollMultiplexer() noexcept;
    virtual void run() override;
    virtual void run_for(std::chrono::milliseconds timeout) override;
    virtual void add_channel(NBIO::Core::ChannelBase* channel) override;
    virtual void delete_channel(NBIO::Core::ChannelBase* channel) noexcept override;

   private:
    void run_impl(int timeout);
    int handle_{-1};                                                                  // epoll file descriptor
    std::unordered_multimap<int, NBIO::Core::ChannelBase*> pollable_channels_;  // all pollable channels
    std::unordered_multimap<int, NBIO::Core::ChannelBase*>
        always_channels_;  // channels that are always ready, non-pollable
    std::vector<ChannelBase*> active_channels_;
};
}  // namespace NBIO::Core
#endif  // defined(__linux__)


