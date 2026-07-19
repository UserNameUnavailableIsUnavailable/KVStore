#include "Client.hpp"

#include <bit>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>

#include "Token.hpp"

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

void Client::ResolveTokens(const std::vector<std::string>& tokens)
{
    if (tokens.empty())
    {
        return;
    }
    if (tokens.size() == 1 && tokens[0] == "exit")
    {
        // TODO: notify the caller that we should exit
        std::exit(0);
    }
    std::cout << "Tokens: ";
    for (const auto& token : tokens)
    {
        std::cout << "[" << token << "] ";
    }
    std::cout << std::endl; // flush the output
}

void Client::Run()
{
    std::cout << prompt_;
    std::string line;
    std::vector<std::string> tokens;
    tokens.reserve(12);
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
        ResolveTokens(tokens);
        std::cout << prompt_;
        tokens.clear();
    } while (true);
}
}