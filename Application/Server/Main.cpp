#include "Server.hpp"

#include <cstdint>
#include <string>

int main(int argc, char *argv[])
{
    const std::uint16_t port = argc > 1 ? static_cast<std::uint16_t>(std::stoul(argv[1])) : 8080;
    KV::Server server;
    server.run(Foundation::Address::from_ipv4("0.0.0.0", port));
}