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

    // Nothing is given a default here: an option that was not named stays empty,
    // which is what lets a startup file set it. The defaults are applied last.
    app.add_option("-p,--port", options.port, "TCP port clients connect to (default 8080)")->check(CLI::Range(1, 65535));
    app.add_option("--replication-port", options.replication_port,
                   "RDMA port this server serves snapshots on (default 0, which serves none)")
        ->check(CLI::Range(0, 65535));
    app.add_option("--replication-ip", options.replication_address,
                   "local address the replication listener binds (default 0.0.0.0)");
    app.add_option("--rdma-device", options.rdma_device,
                   "RDMA device the replication link runs on, by name (--rdma-device siw0)");
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