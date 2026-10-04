#pragma once

#include <nbio/net/Result.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <sys/socket.h>
#include <system_error>
#include <variant>
#include <vector>

namespace nbio::net::detail {
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

template <typename C>
class CommunicationPayload {
   public:
    void bundle() {
        submitted_.insert(submitted_.end(), prepared_.begin(), prepared_.end());
        prepared_.clear();
    }

    std::size_t size() const noexcept { return prepared_.size() + submitted_.size(); }

    void submit(Communication* communication) {
        assert(communication != nullptr);
        prepared_.push_back(communication);
    }

    Communication* NextSubmission() { return submitted_.empty() ? nullptr : submitted_.front(); }

    void Complete() noexcept {
        assert(!submitted_.empty());
        completed_.push_back(submitted_.front());
        submitted_.pop_front();
    }

    Communication* next_completion() { return completed_.empty() ? nullptr : completed_.front(); }

    void conclude() noexcept {
        assert(!completed_.empty());
        completed_.pop_front();
    }

   private:
    std::deque<Communication*> prepared_{};
    std::deque<Communication*> submitted_{};
    std::deque<Communication*> completed_{};
};

template <typename C>
class MessagePayload {
   public:
    ::msghdr& header() noexcept {
        submitted_.insert(submitted_.end(), prepared_.begin(), prepared_.end());
        prepared_.clear();
        vectors_.clear();
        for (auto* trans : submitted_) {
            vectors_.emplace_back(::iovec{.iov_base = trans->buffer.data(), .iov_len = trans->buffer.size()});
        }
        header_.msg_iov = vectors_.data();
        header_.msg_iovlen = vectors_.size();
        return header_;
    }

    std::size_t size() const noexcept { return submitted_.size() + prepared_.size(); }

    void submit(Transmission* transmission) {
        assert(transmission != nullptr);
        prepared_.push_back(transmission);
    }

    Transmission* NextSubmission() { return submitted_.empty() ? nullptr : submitted_.front(); }

    void Complete() noexcept {
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
    ::msghdr header_{};
    std::vector<::iovec> vectors_;
    std::deque<Transmission*> prepared_{};
    std::deque<Transmission*> submitted_{};
    std::deque<Transmission*> completed_{};
};
}  // namespace nbio::net::detail