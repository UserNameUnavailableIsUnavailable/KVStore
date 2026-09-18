#include "Server.hpp"

#include <CLI/CLI.hpp>

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <string>

namespace
{
// `--replicaof` names the master the way an endpoint is written everywhere
// else, so it carries the port with it: <ip>:<port>.
std::optional<Foundation::Core::Address> ParseMaster(const std::string &text)
{
    const auto colon = text.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == text.size())
    {
        throw std::invalid_argument("--replicaof wants <ip>:<port>, not '" + text + "'");
    }

    const auto port = std::stoul(text.substr(colon + 1));
    if (port == 0 || port > 65535)
    {
        throw std::invalid_argument("--replicaof wants a port between 1 and 65535, not '" + text.substr(colon + 1) + "'");
    }
    return Foundation::Core::Address::from_ipv4(text.substr(0, colon), static_cast<std::uint16_t>(port));
}
} // namespace

int main(int argc, char *argv[])
{
    // The server's logs are the only window into a transfer, so the level has to
    // be reachable without a rebuild.
    if (const char *level = std::getenv("KVSTORE_LOG_LEVEL"); level != nullptr && *level != '\0')
    {
        spdlog::set_level(spdlog::level::from_str(level));
    }

    CLI::App app{"KVStore server", "kvstore-server"};

    KV::ServerOptions options;
    std::string replicaof;

    // Nothing is given a default here: an option that was not named stays empty,
    // which is what lets a startup file set it. The defaults are applied last.
    app.add_option("-p,--port", options.port, "TCP port clients connect to (default 8080)")->check(CLI::Range(1, 65535));
    app.add_option("--replication-port", options.replication_port,
                   "RDMA port this server serves snapshots on (default 0, which serves none)")
        ->check(CLI::Range(0, 65535));
    app.add_option("--replication-address", options.replication_address,
                   "local address the replication listener binds (default 0.0.0.0)");
    app.add_option("--replicaof", replicaof, "RDMA <ip>:<port> of the master to replicate");
    app.add_option("--multiplexer", options.multiplexer, "I/O multiplexer: epoll or io_uring")
        ->check(CLI::IsMember({"epoll", "io_uring"}));
    app.add_option("-c,--config", options.config_file, "command file to run at startup, one command per line")
        ->check(CLI::ExistingFile);

    try
    {
        app.parse(argc, argv);
    }
    catch (const CLI::ParseError &error)
    {
        return app.exit(error);
    }

    try
    {
        if (!replicaof.empty())
        {
            options.master = ParseMaster(replicaof);
        }
    }
    catch (const std::exception &error)
    {
        spdlog::error("{}", error.what());
        return 2;
    }

    try
    {
        KV::Server server;
        server.run(options);
    }
    catch (const std::exception &error)
    {
        spdlog::error("server stopped: {}", error.what());
        return 1;
    }
    return 0;
}