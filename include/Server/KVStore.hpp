#pragma once

#include <string>

struct KVS_Request
{
    std::string key;
    std::string value;
};

struct KVS_Response
{
    std::string status;
    std::string value;
};

void KVS_Request();
void KVS_Response();