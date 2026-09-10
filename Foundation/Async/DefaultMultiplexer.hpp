#pragma once

#if defined(__linux__)
#include "EpollMultiplexer.hpp"
#endif

namespace Foundation::Async
{
#if defined(__linux__)
using DefaultMultiplexer = EpollMultiplexer;
#else
#error "Unsupported platform"
#endif
} // namespace Foundation::Async
