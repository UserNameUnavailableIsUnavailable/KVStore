#include <storage/write_log.hpp>

// Keep both the symbol and call under optimization/LTO. The memory operand also
// prevents payload construction from moving past the observation boundary.
extern "C" __attribute__((noinline, used, visibility("default")))
#if defined(__GNUC__) && !defined(__clang__)
__attribute__((noipa))
#endif
void kvstore_write_log_v1(std::uint64_t epoch, std::uint64_t end_offset,
                          const char* payload, std::uint64_t length) noexcept {
    asm volatile("" : : "r"(epoch), "r"(end_offset), "r"(payload), "r"(length) : "memory");
}
