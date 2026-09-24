#pragma once
#if defined(__linux__)

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <set>

#include <chrono>
#include <cstddef>
#include <cstdint>

#include <Foundation/NBIO/Types.hpp>

#include <liburing.h>

namespace Foundation::NBIO
{
class URingMultiplexer final : public Multiplexer
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
    void delete_channel(Foundation::NBIO::Channel *channel) noexcept override;

  private:
    void run_impl(int timeout_ms);

    // Turn the channel's next operation into a submission queue entry. Answers
    // false when the channel has nothing to hand over, an operation is already in
    // flight for it, or the submission queue is full.
    bool prepare(Foundation::NBIO::Channel *channel);
    void submit();
    // Reap every ready completion: spread each outcome over the channel's batch,
    // then ask the channel to wake what it answered and arm what is left.
    void handle_completions();
    io_uring ring_;
    // Channels currently armed: the ones submit() asks for work.
    std::set<Channel*> channels_;
    // Channels whose operation is already with the kernel.
    std::set<Channel*> in_flight_;
};
} // namespace Foundation::NBIO
#endif // defined(__linux__)
