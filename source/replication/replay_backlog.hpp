#pragma once

#include "capture_stream.hpp"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <limits>
#include <string>
#include <utility>
#include <algorithm>

namespace replication {
using Clock = std::chrono::steady_clock;

struct Record {
    std::uint64_t end_offset;
    std::string payload;
    std::size_t Cost() const { return payload.size() + sizeof(Record) + 64; }
};

class ReplayBacklog {
public:
    explicit ReplayBacklog(std::size_t limit) : limit_(limit) {}

    void Invalidate() {
        if (healthy) std::cerr << "repld: stream continuity lost; disconnecting followers\n";
        healthy = false;
        ++generation;
        backlog_.clear();
        bytes_ = 0;
    }

    void Consume(CaptureBatch batch) {
        if (batch.reset) {
            Invalidate();
            epoch = batch.epoch;
            head = batch.head;
            healthy = true;
            return;
        }
        if (!healthy || batch.epoch != epoch) {
            Invalidate();
            return;
        }
        for (auto& entry : batch.records) {
            if (entry.payload.empty() || entry.payload.size() > UINT64_MAX - head ||
                entry.end_offset != head + entry.payload.size()) {
                Invalidate();
                return;
            }
            Record record{entry.end_offset, std::move(entry.payload)};
            if (record.Cost() > limit_) {
                Invalidate();
                head = record.end_offset;
                healthy = true;
                continue;
            }
            while (!backlog_.empty() && bytes_ > limit_ - record.Cost()) {
                bytes_ -= backlog_.front().Cost();
                backlog_.pop_front();
            }
            bytes_ += record.Cost();
            head = record.end_offset;
            backlog_.push_back(std::move(record));
        }
        if (head != batch.head) Invalidate();
    }

    bool CanFollow(std::uint64_t requested_epoch, std::uint64_t snapshot) const {
        return healthy && requested_epoch == epoch && snapshot <= head &&
            (snapshot == head || Next(snapshot) != nullptr);
    }

    const Record* Next(std::uint64_t last) const {
        if (last >= head) return nullptr;
        const auto next = std::upper_bound(backlog_.begin(), backlog_.end(), last,
            [](std::uint64_t offset, const Record& record) { return offset < record.end_offset; });
        return next != backlog_.end() && next->end_offset - next->payload.size() == last ? &*next : nullptr;
    }

    bool healthy = false;
    std::uint64_t epoch = 0;
    std::uint64_t head = 0;
    std::uint64_t generation = 0;

private:
    std::size_t limit_;
    std::size_t bytes_ = 0;
    std::deque<Record> backlog_;
};
}  // namespace replication
