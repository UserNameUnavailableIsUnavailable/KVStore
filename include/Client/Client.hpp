#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

#include "Common/Command.hpp"

namespace KV
{

class Client
{
public:
    Client();
    ~Client();
    void SetPrompt(std::string prompt)
    {
        prompt_ = std::move(prompt);
    }
    const std::string& GetPrompt() const
    {
        return prompt_;
    }
    void Connect(const char* host, std::uint16_t port);
    void Run();
private:
    std::optional<KV::Command> ResolveTokens(const std::vector<std::string>& tokens);

    std::string prompt_ = "KV> ";
    int client_fd_ = -1;
};
} // namespace KV
