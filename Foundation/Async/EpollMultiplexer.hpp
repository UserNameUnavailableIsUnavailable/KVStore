#pragma once

#include <chrono>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "Channel.hpp"
#include "Multiplexer.hpp"
#include "Types.hpp"

#if not defined(__linux__)
#error "EpollMultiplexer is only supported on Linux"
#endif

#include <sys/epoll.h>

namespace Foundation::Async
{
class EpollMultiplexer final : public Multiplexer
{
  public:
    using Handle = int;
    EpollMultiplexer();
    virtual ~EpollMultiplexer() noexcept;
    virtual void run() override;
    virtual void run_for(std::chrono::milliseconds timeout) override;
    virtual void add_channel(Channel *channel) override;
    virtual void update_channel(Channel *channel) override;
    virtual void delete_channel(Channel *channel) noexcept override;
    Handle native_handle() const noexcept
    {
        return handle_;
    }
    virtual MultiplexerType type() const override
    {
        return MultiplexerType::kEpoll;
    }

  private:
    void run_impl(int timeout);

    // Maps a channel type to the corresponding epoll event flag.
    static std::uint32_t native_flags_for(ChannelType type);

    // Returns the combined flags for the given fd's associated ARMED channels.
    std::uint32_t combine_flags_for(int fd) const;

    // Syncs the registration of the given fd with the kernel.
    void sync_registration(int fd, bool already_added);

    int handle_{-1}; // epoll file descriptor
    std::unordered_multimap<int, Channel *>
        registered_channels_;                // fd -> channel, a fd may be associated with multiple channels
    std::vector<Channel *> active_channels_; // channels that are ready for I/O
    std::vector<Channel *> always_ready_channels_;
};
} // namespace Foundation::Async
