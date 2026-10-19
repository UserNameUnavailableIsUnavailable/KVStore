#pragma once

#include <cstdint>

// Versioned C ABI for synchronous observation; payload lives through the call.
// A zero-length call publishes liveness and the current logical offset.
extern "C" void kvstore_write_log_v1(std::uint64_t epoch, std::uint64_t end_offset,
                                     const char* payload, std::uint64_t length) noexcept;
