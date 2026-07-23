#include "Client/Client.hpp"

#include <bit>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <format>

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "Common/Token.hpp"
#include "Common/Result.hpp"

static std::unordered_map<std::uint32_t, std::uint32_t> escape_char = {
    {'r', '\r'},
    {'n', '\n'},
    {'t', '\t'},
    {'\\', '\\'},
    {'\'', '\''},
    {'\"', '\"'}
};

namespace KV
{
Client::Client()
{

}

Client::~Client()
{
    if (client_fd_ >= 0)
    {
        close(client_fd_);
    }
}

void Client::Connect(const char* host, std::uint16_t port)
{
    sockaddr_in addr {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {},
        .sin_zero = {}
    };
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0)
    {
        throw std::runtime_error(std::format("invalid address: {}", host));
    }
    client_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd_ < 0)
    {
        throw std::runtime_error("failed to create socket");
    }
    if (connect(client_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        throw std::runtime_error(std::format("failed to connect to {}:{}", host, port));
    }
}

std::optional<KV::Command> Client::ResolveTokens(const std::vector<std::string>& tokens)
{
    if (tokens.empty())
    {
        return std::nullopt;
    }
    return KV::Command(tokens[0], std::vector<std::string>(tokens.begin() + 1, tokens.end()));
}

void Client::Run()
{
    if (client_fd_ < 0)
    {
        throw std::runtime_error("client is not connected to a server");
    }
    std::cout << prompt_;
    std::string line;
    std::vector<std::string> tokens;
    tokens.reserve(12);
    bool bye = false;
    do
    {
        std::string token; // current token parsed
        std::getline(std::cin, line);
        // split line into tokens
        char quote = 0; // the quote char
        bool escaped = false;
        // whether the current token is resolved (not inside quotes / escape char pending)
        auto pending = [&quote, &escaped]() -> bool {
            return quote != 0 || escaped;
        };
        for (auto it = line.begin(); it != line.end();)
        {
            std::string_view sv(it, line.end());
            auto cp = Macrohard::ResolveNextCodePoint(sv);
            if (!cp.has_value())
            {
                std::cerr << "Invalid UTF-8 sequence" << std::endl;
                break;
            }
            it += cp->length;
            
            std::uint32_t first_byte = cp->representation[0];
            if (escaped)
            {
                if (cp->length != 1 || !escape_char.contains(cp->representation[0]))
                {
                    for (std::uint32_t i = 0; i < cp->length; ++i)
                    {
                        token += static_cast<char>(cp->representation[i]);
                    }
                }
                else
                {
                    token += escape_char[cp->representation[0]];
                }
                escaped = false;
            }
            else if (first_byte == '\\')
            {
                escaped = true;
            }
            else if (first_byte == '\'' || first_byte == '\"')
            {
                if (quote == 0)
                {
                    quote = first_byte;
                }
                // closing quote
                else if (std::bit_cast<std::uint8_t>(quote) == first_byte)
                {
                    quote = 0;
                    tokens.push_back(std::move(token));
                    token.clear();
                }
                else
                {
                    token += static_cast<char>(first_byte);
                }
            }
            else if (std::isspace(first_byte))
            {
                if (pending())
                {
                    token += static_cast<char>(first_byte);
                }
                else if (!token.empty())
                {
                    tokens.push_back(std::move(token));
                    token.clear();
                }
            }
            else
            {
                for (std::uint32_t i = 0; i < cp->length; ++i)
                {
                    token += static_cast<char>(cp->representation[i]);
                }
            }
        }
        if (pending())
        {
            std::cerr << "Unterminated quote or escape sequence." << std::endl;
        }
        else if (!token.empty())
        {
            tokens.push_back(std::move(token));
        }
        auto cmd = ResolveTokens(tokens);
        if (cmd.has_value())
        {
            if (cmd->GetName() == "exit")
            {
                bye = true;
                std::cout << "Bye!" << std::endl;
                break;
            }
            else
            {
                std::string serialized = cmd->Serialize();
                ssize_t sent = send(client_fd_, serialized.data(), serialized.size(), 0);
                if (sent < 0)
                {
                    std::cerr << "Failed to send command to server." << std::endl;
                    continue;
                }
                char buffer[1024];
                // TODO: handle partial responses and multiple responses
                ssize_t received = recv(client_fd_, buffer, sizeof(buffer) - 1, 0);
                if (received < 0)
                {
                    std::cerr << "Failed to receive response from server." << std::endl;
                    continue;
                }
                if (received == 0)
                {
                    std::cerr << "Server closed the connection." << std::endl;
                    break;
                }
                KV::Result response;
                response.Deserialize(std::string(buffer, static_cast<std::size_t>(received)));
                std::cout << "Server response: " << response.GetMessage();
                if (!response.GetResult().empty())
                {
                    std::cout << "\n" << response.GetResult();
                }
                std::cout << std::endl;
            }
        }
        std::cout << prompt_;
        tokens.clear();
    } while (!bye);
}
}
