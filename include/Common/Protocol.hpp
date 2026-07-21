#pragma once

#include <string>
#include <vector>

namespace KV
{

struct Request
{
    std::string command;
    std::vector<std::string> arguments;
};

struct Response
{
    std::string message;
};

} // KV
