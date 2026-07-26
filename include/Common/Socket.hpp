#pragma once

#if defined(__linux__)
using SocketHandleType = int;
#elif defined(_WIN32)
#include <winsock2.h>
using SocketHandleType = SOCKET;
#else
#error "Unsupported platform"
#endif