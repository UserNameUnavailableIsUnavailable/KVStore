#pragma once

#include <cstdint>
#include <span>
#include <stdexcept>

namespace nbio::net {
template <typename Header, typename PayloadUnit>
    requires std::is_trivially_destructible_v<Header> && std::is_trivially_destructible_v<PayloadUnit> &&
             (sizeof(Header) % alignof(PayloadUnit) == 0)
class PacketView {
   public:
    PacketView(void* raw, std::size_t size_in_bytes) : raw_(raw), size_in_bytes_(size_in_bytes) {
        if (size_in_bytes_ < sizeof(Header)) [[unlikely]] {
            throw std::logic_error("the raw memory chunk is insufficient to contain a header");
        }
        if (reinterpret_cast<std::uintptr_t>(raw_) % alignof(Header) != 0) [[unlikely]] {
            throw std::invalid_argument("raw memory not aligned for Header");
        }

        if (reinterpret_cast<std::uintptr_t>(raw_) % alignof(PayloadUnit) != 0) [[unlikely]] {
            throw std::invalid_argument("raw memory not aligned for PayloadUnit");
        }
    }

    ~PacketView() noexcept = default;

    const Header& header() const noexcept { return *static_cast<Header*>(raw_); }

    Header& header() noexcept { return *static_cast<Header*>(raw_); }

    std::span<PayloadUnit> payload() noexcept {
        auto base = reinterpret_cast<PayloadUnit*>(static_cast<std::byte*>(raw_) + sizeof(Header));
        return {base, (size_in_bytes_ - sizeof(Header)) / sizeof(PayloadUnit)};
    }

    std::span<const PayloadUnit> payload() const noexcept {
        auto base = reinterpret_cast<const PayloadUnit*>(static_cast<const std::byte*>(raw_) + sizeof(Header));
        return {base, (size_in_bytes_ - sizeof(Header)) / sizeof(PayloadUnit)};
    }

   private:
    void* raw_;
    std::size_t size_in_bytes_;
};
}  // namespace nbio::net

