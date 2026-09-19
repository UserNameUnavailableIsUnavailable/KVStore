#pragma once

#include <chrono>
#include <cstdint>

#include "Types.hpp"

namespace Foundation::NBIO
{
class Channel;

class Multiplexer
{
  public:
    explicit Multiplexer(MultiplexerType type) :
		type_(type)
    {
    }
    virtual ~Multiplexer() = default;
    virtual void add_channel(Channel *channel) = 0;
    virtual void update_channel(Channel *channel) = 0;
    virtual void delete_channel(Channel *channel) noexcept = 0;
    virtual void run_for(std::chrono::milliseconds timeout) = 0;
    virtual void run() = 0;
	MultiplexerType type() const noexcept
	{
		return type_;
	}

    bool event_model() const noexcept
    {
#if defined(__linux__)
        return type_ == MultiplexerType::kEpoll;
#else
        return false;
#endif
    }
  private:
	MultiplexerType type_;
};
} // namespace Foundation::NBIO
