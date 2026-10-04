#pragma once


#include <cassert>
#include <cstddef>
#include <deque>

namespace NBIO::Core::detail {

// The channels that only poll carry no data: their whole wait is a one-shot poll
// of a descriptor they drain themselves. The payload is just whether a poll is
// already out there, which is what stops the backend handing over a second one.
template <typename C>
class PollPayload {
   public:
    bool wants_poll() const noexcept { return !submitted_; }
    void take_poll() noexcept { submitted_ = true; }
    void release_poll() noexcept { submitted_ = false; }
    bool outstanding() const noexcept { return submitted_; }

   private:
    bool submitted_{false};
};
}  // namespace NBIO::Core::detail

