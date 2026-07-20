#include "Client.hpp"

#include <iostream>

int main(int argc, char* argv[])
{
    KV::Client client;
    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " <host> <port>" << std::endl;
        return 1;
    }
    const char* host = argv[1];
    std::uint16_t port = static_cast<std::uint16_t>(std::stoi(argv[2]));
    client.Connect(host, port);
    client.Run();
}