#pragma once

namespace NBIO::RDMA::detail {
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
}  // namespace NBIO::RDMA::detail
