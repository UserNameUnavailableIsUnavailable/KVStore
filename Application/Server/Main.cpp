#include <spdlog/spdlog.h>

#include <CLI/CLI.hpp>
#include <Foundation/NBIO/EpollMultiplexer.hpp>
#include <Foundation/NBIO/NBIO.hpp>
#include <Foundation/NBIO/URingMultiplexer.hpp>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>

#include "Server.hpp"

namespace {
std::unique_ptr<Foundation::NBIO::Multiplexer> make_multiplexer(const std::string& name) {
    if (name == "epoll") {
        return std::make_unique<Foundation::NBIO::EpollMultiplexer>();
    }
    if (name == "io_uring") {
        return std::make_unique<Foundation::NBIO::URingMultiplexer>();
    }
    throw std::invalid_argument("--multiplexer must be 'epoll' or 'io_uring'");
}
}  // namespace

int main(int argc, char* argv[]) {
    // The server's logs are the only window into a transfer, so the level has to
    // be reachable without a rebuild.
    if (const char* level = std::getenv("KVSTORE_LOG_LEVEL"); level != nullptr && *level != '\0') {
        spdlog::set_level(spdlog::level::from_str(level));
    }

    CLI::App app{"KVStore server", "kvstore-server"};

    KV::ServerOptions options;

    // Nothing is given a default here: an option that was not named stays empty,
    // which is what lets a startup file set it. The defaults are applied last.
    app.add_option("-p,--port", options.port, "TCP port clients connect to (default 8080)")
        ->check(CLI::Range(1, 65535));
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

    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        return app.exit(error);
    }

    try {
        auto mux = make_multiplexer(options.multiplexer);
        switch (mux->type()) {
            case Foundation::NBIO::MultiplexerType::kEpoll:
                spdlog::info("Multiplexer: epoll");
                break;
            case Foundation::NBIO::MultiplexerType::kURing:
                spdlog::info("Multiplexer: io_uring");
                break;
        }
        Foundation::NBIO::initialize(std::move(mux));

        KV::Server server;
        server.run(options);
    } catch (const std::exception& error) {
        spdlog::error("server stopped: {}", error.what());
        return 1;
    }
    return 0;
}