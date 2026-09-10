#pragma once

#include <chrono>
#include <cstdint>

#include "Types.hpp"

namespace Foundation::Async
{
class SessionService;
class Channel;
struct EventSet;
class Scheduler;

class Multiplexer
{
  public:
    explicit Multiplexer()
    {
    }
    virtual ~Multiplexer() = default;
    virtual void add_channel(Channel *channel) = 0;
    virtual void update_channel(Channel *channel) = 0;
    virtual void delete_channel(Channel *channel) noexcept = 0;
    virtual void run_for(std::chrono::milliseconds timeout) = 0;
    virtual void run() = 0;
    virtual MultiplexerType type() const = 0;
};
} // namespace Foundation::Async
