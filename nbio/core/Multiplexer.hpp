#pragma once

#include <chrono>

#include "Types.hpp"

namespace nbio::core {
class ChannelBase;

class Multiplexer {
   public:
    explicit Multiplexer(MultiplexerType type) : type_(type) {}
    virtual ~Multiplexer() = default;
    virtual void AddChannel(ChannelBase* channel) = 0;
    virtual void DeleteChannel(ChannelBase* channel) noexcept = 0;
    virtual void RunFor(std::chrono::milliseconds timeout) = 0;
    virtual void Run() = 0;

    MultiplexerType type() const noexcept { return type_; }

   private:
    MultiplexerType type_;
};
}  // namespace nbio::core


