#pragma once

#include <chrono>
#include <cstdint>

#include "Channel.hpp"
#include "Multiplexer.hpp"
#include "Types.hpp"

#if not defined(__linux__)
#error "URingMultiplexer is only supported on Linux"
#endif

#include "liburing.h"

namespace Foundation::Async
{
class URingMultiplexer final : public Multiplexer
{
  public:
    using Handle = io_uring *;

    explicit URingMultiplexer(std::uint32_t submission_capacity = 1024, std::uint32_t completion_capacity = 2048);
    URingMultiplexer(const URingMultiplexer &) = delete;
    URingMultiplexer &operator=(const URingMultiplexer &) = delete;
    ~URingMultiplexer() noexcept override;

    void run() override;
    void run_for(std::chrono::milliseconds timeout) override;
    void add_channel(Channel *channel) override;
    void update_channel(Channel *channel) override;
    void delete_channel(Channel *channel) noexcept override;

    MultiplexerType type() const override
    {
        return MultiplexerType::kURing;
    }
    Handle native_handle() noexcept
    {
        return &ring_;
    }

  private:
    void run_impl(int timeout_ms);

    // Turn the channel's armed operation into a submission queue entry.
    // Returns false when the submission queue is full; the caller retries later.
    bool prepare(Channel *channel);
    void submit();
    // Write one completion's outcome into the channel's job.
    static void complete(Channel *channel, int result);
    // Reap every ready completion: fill the job, then dispatch to the channel.
    void handle_completions();

    io_uring ring_{};
    // fd -> channels living on that fd (simplex channels share a socket).
    std::unordered_multimap<int, Channel *> registered_channels_;
    // Channels whose operation is armed but not yet handed to the kernel. FIFO,
    // so a burst of submissions cannot starve any single channel.
    std::vector<Channel *> pending_submissions_;
};
} // namespace Foundation::Async
