#pragma once

#include "Session.hpp"

#if not defined (__linux__)
#error "This header is linux-specific."
#endif

namespace KV
{
class EpollSession final : public Session
{
public:
    EpollSession() : Session(NetworkingModel::kReactor) {}
};
} // namespace KV
