#pragma once

#include <string>
#include <vector>

namespace KV
{

class Request
{
public:
    std::string command;
    std::vector<std::string> arguments;
};

class Response
{
public:
    std::string message;
};

} // KV
