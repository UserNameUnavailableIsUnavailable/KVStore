#pragma once
#if defined(__linux__)

#include <sys/epoll.h>

#include <nbio/async/Scheduler.hpp>
#include <nbio/async/Task.hpp>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Channel.hpp"
#include "Multiplexer.hpp"

namespace nbio::core {
class EpollMultiplexer final : public nbio::core::Multiplexer {
   public:
    using Handle = int;
    EpollMultiplexer();
    virtual ~EpollMultiplexer() noexcept;
    virtual void Run() override;
    virtual void RunFor(std::chrono::milliseconds timeout) override;
    virtual void AddChannel(nbio::core::ChannelBase* channel) override;
    virtual void DeleteChannel(nbio::core::ChannelBase* channel) noexcept override;

   private:
    void RunImpl(int timeout);
    int handle_{-1};                                                                  // epoll file descriptor
    std::unordered_multimap<int, nbio::core::ChannelBase*> pollable_channels_;  // all pollable channels
    std::unordered_multimap<int, nbio::core::ChannelBase*>
        always_channels_;  // channels that are always ready, non-pollable
    std::vector<ChannelBase*> active_channels_;
};
}  // namespace nbio::core
#endif  // defined(__linux__)


