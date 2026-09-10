#include "TimerService.hpp"

namespace Foundation::Async
{
TimerService::TimerService(Multiplexer &multiplexer, Scheduler &scheduler)
    : timer_(), channel_(timer_, multiplexer, scheduler)
{
}
} // namespace Foundation::Async
