#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <stdexcept>
#include <string>
#include <string_view>

namespace replication {
class CommandBacklog {
public:
    struct Record {
        std::uint64_t offset;
        std::string payload;
    };

    explicit CommandBacklog(std::size_t capacity = 64U * 1024U * 1024U) : capacity_(capacity) {}

    void Reset(std::uint64_t offset) {
        records_.clear();
        bytes_ = 0;
        head_ = offset;
    }

    void Append(std::uint64_t offset, std::string_view payload) {
        if (head_ == UINT64_MAX || offset != head_ + 1 || payload.empty()) {
            throw std::runtime_error("replication backlog is discontinuous");
        }
        const auto cost = payload.size() + sizeof(Record);
        if (cost > capacity_) {
            Reset(offset);
            return;
        }
        while (bytes_ > capacity_ - cost) {
            bytes_ -= records_.front().payload.size() + sizeof(Record);
            records_.pop_front();
        }
        records_.push_back({offset, std::string(payload)});
        bytes_ += cost;
        head_ = offset;
    }

    bool Contains(std::uint64_t last) const noexcept {
        return last <= head_ && (last == head_ ||
            (!records_.empty() && last >= records_.front().offset - 1));
    }

    const Record* Next(std::uint64_t last) const noexcept {
        if (!Contains(last) || last == head_) return nullptr;
        return &records_[static_cast<std::size_t>(last + 1 - records_.front().offset)];
    }

    std::uint64_t head() const noexcept { return head_; }

private:
    std::deque<Record> records_;
    std::size_t bytes_ = 0;
    std::size_t capacity_;
    std::uint64_t head_ = 0;
};
}