#pragma once
#if defined(__linux__)

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <chrono>
#include <cstdint>

#include <Foundation/NBIO/Types.hpp>

#include <liburing.h>

namespace Foundation::NBIO
{
class URingMultiplexer final : public Foundation::NBIO::Multiplexer
{
  public:
    using Handle = io_uring *;

    explicit URingMultiplexer(std::uint32_t submission_capacity = 8291, std::uint32_t completion_capacity = 16384);
    URingMultiplexer(const URingMultiplexer &) = delete;
    URingMultiplexer &operator=(const URingMultiplexer &) = delete;
    ~URingMultiplexer() noexcept override;

    void run() override;
    void run_for(std::chrono::milliseconds timeout) override;
    void add_channel(Foundation::NBIO::Channel *channel) override;
    void update_channel(Foundation::NBIO::Channel *channel) override;
    void delete_channel(Foundation::NBIO::Channel *channel) noexcept override;

  private:
    void run_impl(int timeout_ms);

    // Turn the channel's armed operation into a submission queue entry.
    // Returns false when the submission queue is full; the caller retries later.
    bool prepare(Foundation::NBIO::Channel *channel);
    void submit();
    // Write one completion's outcome into the channel's job.
    static void complete(Foundation::NBIO::Channel *channel, int result);
    // Reap every ready completion: fill the job, then dispatch to the channel.
    void handle_completions();
    io_uring ring_;
    // fd -> channels living on that fd (simplex channels share a socket).
    std::unordered_multimap<int, Foundation::NBIO::Channel *> registered_channels_;
    // Channels whose operation is armed but not yet handed to the kernel. FIFO,
    // so a burst of submissions cannot starve any single channel.
    std::vector<Foundation::NBIO::Channel *> pending_submissions_;
};
} // namespace Foundation::NBIO
#endif // defined(__linux__)
