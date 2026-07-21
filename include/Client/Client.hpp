#pragma once

#include <cstdint>
#include <sstream>
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
    std::string Serialize() const
    {
        std::stringstream ss;
        // RESP-like serialization
        ss << arguments_.size() + 1 << "\r\n"; // number of tokens (command + arguments)
        ss << name_.length() << "\r\n"; // length of command
        ss << name_ << "\r\n";
        for (const auto& arg : arguments_)
        {
            ss << arg.length() << "\r\n";
            ss << arg << "\r\n";
        }
        return ss.str();
    }
private:
    std::string name_;
    std::vector<std::string> arguments_;
};

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