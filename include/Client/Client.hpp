#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

#include "Command.hpp"
#include <Foundation/Socket.hpp>

namespace KV
{

class Client
{
public:
    Client() = default;
    ~Client() = default;
    void SetPrompt(std::string prompt)
    {
        prompt_ = std::move(prompt);
    }
    const std::string& GetPrompt() const
    {
        return prompt_;
    }
    void Connect(const char* host, std::uint16_t port);
    void run();
private:
    std::optional<KV::Command> resolveTokens(const std::vector<std::string>& tokens);

    std::string prompt_ = "KV> ";
    Socket socket_;
};
} // namespace KV
