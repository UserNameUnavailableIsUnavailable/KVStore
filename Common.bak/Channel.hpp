#pragma once

#include <cstdint>
#include <span>

namespace KV
{

class Channel
{
public:
    Channel();
    virtual ~Channel() noexcept = default;
    constexpr static std::uint32_t kRead = 0b1;
    constexpr static std::uint32_t kWrite = 0b01;
    virtual std::uint32_t Read(std::span<char> buffer) = 0;
    virtual void Write(std::span<const char> buffer) = 0;
};
};
