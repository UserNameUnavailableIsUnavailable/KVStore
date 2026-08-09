#pragma once

#include <string>
#include <vector>

namespace KV
{
struct Command
{
    std::pmr::string name;
    std::pmr::vector<std::pmr::string> arguments;
};
} // namespace KV
