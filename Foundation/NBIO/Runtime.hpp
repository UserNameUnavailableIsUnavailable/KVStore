#pragma once

#include <Foundation/Async/Task.hpp>

namespace Foundation::NBIO
{
class Engine;
using Runtime = Engine;
template <typename T> using Task = Foundation::Async::Task<Runtime, T>;
} // namespace Foundation::NBIO
