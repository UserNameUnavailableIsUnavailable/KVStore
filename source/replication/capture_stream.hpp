#pragma once

#include <replication/probe_event.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace replication {
struct CapturedRecord {
    std::uint64_t end_offset;
    std::string payload;
};

struct CaptureBatch {
    std::uint64_t epoch = 0;
    std::uint64_t head = 0;
    bool reset = false;
    std::vector<CapturedRecord> records;
};

class CaptureStream {
public:
    std::optional<CaptureBatch> Observe(const kv_log_state& state) {
        if (!state.epoch) return std::nullopt;
        if (epoch_ == state.epoch && failures_ == state.failures) return std::nullopt;
        failures_ = state.failures;
        return Reset(state.epoch, state.head);
    }

    CaptureBatch Reset(std::uint64_t epoch, std::uint64_t head) {
        epoch_ = epoch;
        head_ = head;
        payload_.clear();
        return {epoch, head, true, {}};
    }

    CaptureBatch Consume(const kv_log_event& event) {
        if (event.epoch != epoch_) throw std::runtime_error("probe epoch changed");
        if (event.end_offset <= head_) return {epoch_, head_, false, {}};
        if (!event.length || event.length > KV_LOG_MAX_PAYLOAD ||
            event.end_offset < event.length || event.end_offset - event.length != head_ ||
            !event.size || event.size > KV_LOG_CHUNK || event.position > event.length ||
            event.size > event.length - event.position || event.position != payload_.size()) {
            throw std::runtime_error("invalid or noncontiguous write-log probe chunk");
        }
        if (payload_.empty()) {
            end_ = event.end_offset;
            length_ = event.length;
        } else if (end_ != event.end_offset || length_ != event.length) {
            throw std::runtime_error("interleaved write-log probe records");
        }
        payload_.append(event.data, event.size);
        CaptureBatch batch{epoch_, head_, false, {}};
        if (payload_.size() == length_) {
            head_ = end_;
            batch.head = head_;
            batch.records.push_back({head_, std::move(payload_)});
            payload_.clear();
        }
        return batch;
    }
private:
    std::uint64_t epoch_ = 0;
    std::uint64_t head_ = 0;
    std::uint64_t end_ = 0;
    std::uint64_t length_ = 0;
    std::uint64_t failures_ = 0;
    std::string payload_;
};
}  // namespace replication
