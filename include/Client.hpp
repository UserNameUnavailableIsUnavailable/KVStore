#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace KV
{
class Client
{
public:
    Client();
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
    void ResolveTokens(const std::vector<std::string>& tokens);
    std::string prompt_ = "KV> ";
};
} // namespace KV