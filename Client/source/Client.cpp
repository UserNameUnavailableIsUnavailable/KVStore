#include "Client/Client.hpp"

#include <bit>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <stdexcept>

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "Common/Token.hpp"
#include "Common/Result.hpp"
#include "Common/Protocol.hpp"

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
void Client::Connect(const char* host, std::uint16_t port)
{
    Socket socket(SocketProtocol::kTcp);
    socket.Connect(host, port);
    socket_ = std::move(socket);
}

std::optional<KV::Command> Client::ResolveTokens(const std::vector<std::string>& tokens)
{
    if (tokens.empty())
    {
        return std::nullopt;
    }
    Command command {.name = std::pmr::string(tokens[0]), .arguments = {}};
    command.arguments.reserve(tokens.size() - 1);
    for (auto it = tokens.begin() + 1; it != tokens.end(); ++it)
    {
        command.arguments.emplace_back(*it);
    }
    return command;
}

void Client::Run()
{
    if (!socket_.IsOpen())
    {
        throw std::runtime_error("client is not connected to a server");
    }
    std::cout << prompt_;
    std::string line;
    std::vector<std::string> tokens;
    tokens.reserve(12);
    Protocol protocol;
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
            auto cp = KV::ResolveNextCodePoint(sv);
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
            if (cmd->name == "exit" || cmd->name == "EXIT")
            {
                bye = true;
                std::cout << "Bye!" << std::endl;
                break;
            }
            else
            {
                const std::pmr::string serialized = protocol.EncodeRequest(*cmd);
                std::ptrdiff_t sent = send(socket_.GetNativeHandle(), serialized.data(), serialized.size(), 0);
                if (sent < 0)
                {
                    std::cerr << "Failed to send command to server." << std::endl;
                    continue;
                }
                char buffer[1024];
                // TODO: handle partial responses and multiple responses
                std::ptrdiff_t received = recv(socket_.GetNativeHandle(), buffer, sizeof(buffer) - 1, 0);
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
                const ResponseDecode parsed =
                    protocol.DecodeResponse(std::string_view(buffer, static_cast<std::size_t>(received)));
                if (parsed.status != DecodeStatus::kComplete)
                {
                    const std::string reason =
                        parsed.error.empty() ? "incomplete response" : std::string(parsed.error);
                    std::cerr << "Malformed response from server: " << reason << std::endl;
                    continue;
                }

                const auto print_result = [&](const auto& self, const Result& response) -> void
                {
                    if (response.type == ResultType::kArray)
                    {
                        for (const Result& element : response.elements)
                        {
                            self(self, element);
                            std::cout << '\n';
                        }
                        return;
                    }
                    std::cout << (response.type == ResultType::kError ? "ERR " : "") << response.value;
                };
                print_result(print_result, parsed.result);
                std::cout << std::endl;
            }
        }
        std::cout << prompt_;
        tokens.clear();
    } while (!bye);
}
}
