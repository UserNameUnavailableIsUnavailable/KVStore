#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

namespace KV
{
class Command
{
public:
    Command(std::string name, std::vector<std::string> args) :
        name_(std::move(name)),
        arguments_(std::move(args))
    {
    }
public:
    const std::string& GetName() const
    {
        return name_;
    }
    const std::vector<std::string>& GetArguments() const
    {
        return arguments_;
    }
private:
    std::string name_;
    std::vector<std::string> arguments_;
};

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
    std::optional<KV::Command> ResolveTokens(const std::vector<std::string>& tokens);
    std::string prompt_ = "KV> ";
};
} // namespace KV