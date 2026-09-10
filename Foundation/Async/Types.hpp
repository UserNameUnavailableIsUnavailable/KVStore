#pragma once

namespace Foundation::Async
{
enum class MultiplexerType
{
#if defined(__linux__)
    kEpoll,
    kURing
#elif defined(WIN32)
    kIOCP
#endif
};

// Every channel is simplex: it is dedicated to exactly one event. A connection
// therefore owns two channels (receive and send) over the same socket, and a
// listener owns one (accept).
enum class ChannelType
{
    kReceive,
    kSend,
    kListen,
    kTimer,
    kSignal,
    kNotify,
};
} // namespace Foundation::Async
