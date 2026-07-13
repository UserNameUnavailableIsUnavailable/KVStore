#pragma once

#include <string>

namespace KV
{
    enum class TaskType
    {
        kExists,
        kGet,
        kSet,
        kDelete,
        kUpdate
    };

    struct Task
    {
        TaskType type;
        std::string key;
        std::string value;
    };
};
