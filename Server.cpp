#include "Server.hpp"

namespace KV
{

Server::Server()
{
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0)
    {
        throw std::runtime_error(std::format("failed to create socket, errno: {}", errno));
    }

    int yes = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
    {
        throw std::runtime_error(std::format("failed to set SO_REUSEADDR, errno: {}", errno));
    }
}

void Server::Bind(std::uint16_t port)
{
    sockaddr_in addr {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {
            .s_addr = htonl(INADDR_ANY),
        },
        .sin_zero = {}
    };
    int ret = bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (ret < 0)
    {
        throw std::runtime_error((std::format("failed to bind to port {}, errno: {}", port, errno)));
    }
    port_ = port;
    bound_ = true;
}


} // namespace KV