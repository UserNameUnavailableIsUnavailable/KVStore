#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <chrono>
#include <cstdint>
#include <unordered_map>
#include <vector>

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
    virtual Foundation::NBIO::MultiplexerType type() const override
    {
        return Foundation::NBIO::MultiplexerType::kEpoll;
    }

  private:
    void run_impl(int timeout);

    // Maps a channel type to the corresponding epoll event flag.
    static std::uint32_t native_flags_for(Foundation::NBIO::ChannelType type);

    int handle_{-1}; // epoll file descriptor
    std::unordered_map<int, Foundation::NBIO::Channel *> registered_channels_;
    std::vector<Foundation::NBIO::Channel *> active_channels_; // channels that are ready for I/O
    std::vector<Foundation::NBIO::Channel *> always_ready_channels_; // it is uncommon to add many always ready channels, use cache-friendly continuous storage
};
} // namespace Foundation::NBIO
