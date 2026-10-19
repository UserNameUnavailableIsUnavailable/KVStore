#pragma once

#include <charconv>
#include <cstdint>
#include <optional>
#include <string_view>

namespace KV::Replication {
inline constexpr std::uint64_t kMaxPayload = 16U * 1024U * 1024U;

inline bool Unsigned(std::string_view text, std::uint64_t& value) {
    if (text.empty()) return false;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

struct Frame {
    std::uint64_t epoch;
    std::uint64_t end_offset;
    std::uint64_t length;
};

inline std::optional<Frame> ParseFrame(std::string_view line) {
    if (!line.starts_with('@')) return std::nullopt;
    line.remove_prefix(1);
    const auto first = line.find(' ');
    if (first == std::string_view::npos) return std::nullopt;
    const auto second = line.find(' ', first + 1);
    if (second == std::string_view::npos) return std::nullopt;
    Frame frame{};
    if (!Unsigned(line.substr(0, first), frame.epoch) ||
        !Unsigned(line.substr(first + 1, second - first - 1), frame.end_offset) ||
        !Unsigned(line.substr(second + 1), frame.length) || frame.length > kMaxPayload) {
        return std::nullopt;
    }
    return frame;
}

inline bool Follows(const Frame& frame, std::uint64_t epoch, std::uint64_t applied) {
    if (frame.epoch != epoch) return false;
    if (frame.length == 0) return frame.end_offset == applied;
    return frame.length <= UINT64_MAX - applied && frame.end_offset == applied + frame.length;
}
}  // namespace KV::Replication
