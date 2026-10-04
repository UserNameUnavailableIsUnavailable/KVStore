#pragma once
#if defined(__linux__)

#include <liburing.h>

#include <nbio/async/Scheduler.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/core/Channel.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <nbio/core/Types.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <set>

namespace nbio::core {
class URingMultiplexer final : public Multiplexer {
   public:
    using Handle = io_uring*;

    explicit URingMultiplexer(std::uint32_t submission_capacity = 8291, std::uint32_t completion_capacity = 16384);
    URingMultiplexer(const URingMultiplexer&) = delete;
    URingMultiplexer& operator=(const URingMultiplexer&) = delete;
    ~URingMultiplexer() noexcept override;

    void Run() override;
    void RunFor(std::chrono::milliseconds timeout) override;
    void AddChannel(nbio::core::ChannelBase* channel) override;
    void DeleteChannel(nbio::core::ChannelBase* channel) noexcept override;

   private:
    void RunImpl(int timeout_ms);

    // Turn the channel's next operation into a submission queue entry. Answers
    // false when the channel has nothing to hand over, an operation is already in
    // flight for it, or the submission queue is full.
    bool Prepare(nbio::core::ChannelBase* channel);
    void Submit();
    // Reap every ready completion: spread each outcome over the channel's batch,
    // then ask the channel to wake what it answered and arm what is left.
    void HandleCompletions();
    io_uring ring_;
    // Channels currently armed: the ones submit() asks for work.
    std::set<ChannelBase*> channels_;
    // Channels whose operation is already with the kernel.
    std::set<ChannelBase*> in_flight_;
};
}  // namespace nbio::core
#endif  // defined(__linux__)


