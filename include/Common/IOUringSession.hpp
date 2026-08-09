#pragma once

#include "Common/Session.hpp"

namespace KV
{
class IOUringSession final : public Session
{
public:
    explicit IOUringSession(std::pmr::memory_resource* resource = std::pmr::get_default_resource()) :
        Session(NetworkingModel::kProactor, resource)
    {
    }
};
} // namespace KV