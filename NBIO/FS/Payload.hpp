#pragma once

#include <NBIO/FS/Result.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <sys/uio.h>
#include <vector>

namespace NBIO::FS::detail {
template <typename C>
class IOVectorPayload {
   public:
    std::vector<::iovec>& header() noexcept {
        submitted_.insert(submitted_.end(), prepared_.begin(), prepared_.end());
        prepared_.clear();
        vectors_.clear();
        for (auto* trans : submitted_) {
            vectors_.emplace_back(::iovec{.iov_base = trans->buffer.data(), .iov_len = trans->buffer.size()});
        }
        return vectors_;
    }

    std::uint64_t offset() const noexcept { return offset_; }
    void set_offset(std::uint64_t offset) noexcept { offset_ = offset; }
    void advance_offset(std::size_t bytes) noexcept { offset_ += static_cast<std::uint64_t>(bytes); }
    std::size_t size() const noexcept { return submitted_.size() + prepared_.size(); }

    void submit(Transmission* transmission) {
        assert(transmission != nullptr);
        prepared_.push_back(transmission);
    }

    Transmission* next_submission() { return submitted_.empty() ? nullptr : submitted_.front(); }

    void complete() noexcept {
        assert(!submitted_.empty());
        completed_.push_back(submitted_.front());
        submitted_.pop_front();
    }

    Transmission* next_completion() { return completed_.empty() ? nullptr : completed_.front(); }

    void conclude() noexcept {
        assert(!completed_.empty());
        completed_.pop_front();
    }

   private:
    std::uint64_t offset_{0};
    std::vector<::iovec> vectors_;
    std::deque<Transmission*> prepared_{};
    std::deque<Transmission*> submitted_{};
    std::deque<Transmission*> completed_{};
};
}  // namespace NBIO::FS::detail