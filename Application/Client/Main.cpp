#include <Foundation/Core/Address.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/NBIO.hpp>
#include <Foundation/Async/Async.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>

#include <Application/Commands.hpp>
#include <Application/RESP/RESP.hpp>
#include <Application/RESP/Receiver.hpp>
#include <Application/RESP/Sender.hpp>

#include <cctype>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace
{
const std::unordered_map<char, char> kEscapeChar = {
    {'r', '\r'},
    {'n', '\n'},
    {'t', '\t'},
    {'\\', '\\'},
    {'\'', '\''},
    {'"', '"'}
};

std::optional<RESP::Object> MakeRequest(const std::vector<std::string> &tokens)
{
    if (tokens.empty())
    {
        return std::nullopt;
    }

    std::vector<RESP::Object> values;
    values.reserve(tokens.size());
    for (const std::string &token : tokens)
    {
        values.emplace_back(RESP::BulkString{.value = token});
    }
    return RESP::Object(RESP::Array{.values = std::move(values)});
}

void PrintObject(const RESP::Object &object)
{
    std::visit(
        [](const auto &value) {
            using Type = std::decay_t<decltype(value)>;
            if constexpr (std::same_as<Type, RESP::SimpleString>)
            {
                std::cout << value.value;
            }
            else if constexpr (std::same_as<Type, RESP::SimpleError>)
            {
                std::cout << "ERR " << value.value;
            }
            else if constexpr (std::same_as<Type, RESP::Integer>)
            {
                std::cout << value.value;
            }
            else if constexpr (std::same_as<Type, RESP::BulkString>)
            {
                if (value.value.has_value())
                {
                    std::cout << *value.value;
                }
                else
                {
                    std::cout << "(nil)";
                }
            }
            else if constexpr (std::same_as<Type, RESP::Null>)
            {
                std::cout << "(nil)";
            }
            else if constexpr (std::same_as<Type, RESP::Boolean>)
            {
                std::cout << (value.value ? "true" : "false");
            }
            else if constexpr (std::same_as<Type, RESP::Double>)
            {
                std::cout << value.value;
            }
            else if constexpr (std::same_as<Type, RESP::BigNumber>)
            {
                std::cout << value.value;
            }
            else if constexpr (std::same_as<Type, RESP::BulkError>)
            {
                std::cout << value.value;
            }
            else if constexpr (std::same_as<Type, RESP::VerbatimString>)
            {
                std::cout << value.format << ':' << value.value;
            }
            else if constexpr (std::same_as<Type, RESP::Array> || std::same_as<Type, RESP::Set> ||
                               std::same_as<Type, RESP::Push>)
            {
                std::cout << '[';
                for (std::size_t index = 0; index < value.values.size(); ++index)
                {
                    if (index != 0)
                    {
                        std::cout << ", ";
                    }
                    PrintObject(value.values[index]);
                }
                std::cout << ']';
            }
            else if constexpr (std::same_as<Type, RESP::Map> || std::same_as<Type, RESP::Attribute>)
            {
                std::cout << '{';
                for (std::size_t index = 0; index < value.values.size(); ++index)
                {
                    if (index != 0)
                    {
                        std::cout << ", ";
                    }
                    PrintObject(value.values[index].first);
                    std::cout << ": ";
                    PrintObject(value.values[index].second);
                }
                std::cout << '}';
            }
        },
        object.value);
}

std::vector<std::string> Tokenize(const std::string &line)
{
    std::vector<std::string> tokens;
    std::string token;
    char quote = 0;
    bool escaped = false;

    auto push_token = [&]() {
        if (!token.empty())
        {
            tokens.push_back(std::move(token));
            token.clear();
        }
    };

    for (std::size_t index = 0; index < line.size(); ++index)
    {
        const char character = line[index];
        if (escaped)
        {
            const auto escape = kEscapeChar.find(character);
            if (escape != kEscapeChar.end())
            {
                token.push_back(escape->second);
            }
            else
            {
                token.push_back(character);
            }
            escaped = false;
            continue;
        }

        if (character == '\\')
        {
            escaped = true;
            continue;
        }

        if (quote != 0)
        {
            if (character == quote)
            {
                quote = 0;
                push_token();
            }
            else
            {
                token.push_back(character);
            }
            continue;
        }

        if (character == '\'' || character == '"')
        {
            quote = character;
            continue;
        }

        if (std::isspace(static_cast<unsigned char>(character)))
        {
            push_token();
            continue;
        }

        token.push_back(character);
    }

    if (!token.empty())
    {
        tokens.push_back(std::move(token));
    }
    return tokens;
}

} // namespace

Foundation::NBIO::Task<void> run_client(Foundation::Core::Address address, std::string host, std::uint16_t port)
{
    Foundation::Core::Socket socket(address.family(), Foundation::Core::Socket::Type::kStream);
    socket.connect(address);
    auto session = Foundation::NBIO::establish(std::move(socket));

    std::cout << "connected to " << host << ':' << port << '\n';

    std::string line;
    while (std::cout << "> " && std::getline(std::cin, line))
    {
        const std::vector<std::string> tokens = Tokenize(line);
        if (tokens.empty())
        {
            continue;
        }
        if (tokens[0] == "exit" || tokens[0] == "EXIT")
        {
            std::cout << "Bye!" << std::endl;
            break;
        }

        const std::optional<RESP::Object> request = MakeRequest(tokens);
        if (!request.has_value())
        {
            continue;
        }
        const KV::CommandValidation validation = KV::ValidateCommand(*request);
        if (!validation)
        {
            std::cerr << validation.error << '\n';
            continue;
        }

        ::Foundation::Core::Buffer send_buffer;
        RESP::Sender sender(*session, send_buffer);
        if (!co_await sender.send(*request))
        {
            const std::string reason = sender.internal_error().empty() ? "failed to send command" : sender.internal_error();
            std::cerr << reason << '\n';
            co_return;
        }

        ::Foundation::Core::Buffer receive_buffer;
        RESP::Receiver receiver(*session, receive_buffer);
        const std::optional<RESP::Object> response = co_await receiver.receive();
        if (!response)
        {
            const std::string reason = receiver.decode_error().empty() ? receiver.internal_error() : receiver.decode_error();
            std::cerr << "Failed to receive response: " << (reason.empty() ? "server closed the connection" : reason) << '\n';
            co_return;
        }

        PrintObject(*response);
        std::cout << '\n';
    }
}

int main(int argc, char *argv[])
{
    const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    const std::uint16_t port = argc > 2 ? static_cast<std::uint16_t>(std::stoul(argv[2])) : 6379;
    const Foundation::Core::Address address =
        host.find(':') == std::string::npos ? Foundation::Core::Address::from_ipv4(host, port) : Foundation::Core::Address::from_ipv6(host, port);
    Foundation::NBIO::run(run_client(address, host, port));
    return 0;
}
