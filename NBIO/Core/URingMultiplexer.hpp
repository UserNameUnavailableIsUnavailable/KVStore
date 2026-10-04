#pragma once
#if defined(__linux__)

#include <liburing.h>

#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Core/Channel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <NBIO/Core/Types.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <set>

namespace NBIO::Core {
class URingMultiplexer final : public Multiplexer {
   public:
    using Handle = io_uring*;

    explicit URingMultiplexer(std::uint32_t submission_capacity = 8291, std::uint32_t completion_capacity = 16384);
    URingMultiplexer(const URingMultiplexer&) = delete;
    URingMultiplexer& operator=(const URingMultiplexer&) = delete;
    ~URingMultiplexer() noexcept override;

    void run() override;
    void run_for(std::chrono::milliseconds timeout) override;
    void add_channel(NBIO::Core::ChannelBase* channel) override;
    void delete_channel(NBIO::Core::ChannelBase* channel) noexcept override;

   private:
    void run_impl(int timeout_ms);

    // Turn the channel's next operation into a submission queue entry. Answers
    // false when the channel has nothing to hand over, an operation is already in
    // flight for it, or the submission queue is full.
    bool prepare(NBIO::Core::ChannelBase* channel);
    void submit();
    // Reap every ready completion: spread each outcome over the channel's batch,
    // then ask the channel to wake what it answered and arm what is left.
    void handle_completions();
    io_uring ring_;
    // Channels currently armed: the ones submit() asks for work.
    std::set<ChannelBase*> channels_;
    // Channels whose operation is already with the kernel.
    std::set<ChannelBase*> in_flight_;
};
}  // namespace NBIO::Core
#endif  // defined(__linux__)


