#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>

#include <CLI/CLI.hpp>

#include <Foundation/Async/Engine.hpp>
#include "KVServer.hpp"

int main(int argc, char **argv)
{
    try
    {
        unsigned int port = 8080;
        std::string cache_strategy = "hash";
        std::string eviction_policy = "lru";
        std::string persistence_directory = "persistent";
        std::size_t capacity = 0; // 0 == unbounded
        std::string master_host;
        unsigned int master_port = 0;

        CLI::App app("KVStore server (coroutine edition)");
        app.add_option("-p,--port", port, "TCP port to bind")->check(CLI::Range(1U, 65535U));
        app.add_option("-c,--cache-strategy", cache_strategy, "Index implementation")
            ->check(CLI::IsMember({"hash", "array", "red-black-tree", "skip-list"}));
        app.add_option("-e,--eviction-policy", eviction_policy, "Cache eviction policy")
            ->check(CLI::IsMember({"lru", "lfu"}));
        app.add_option("--capacity", capacity, "Max records (0 = unbounded)");
        app.add_option("--persistent-dir", persistence_directory, "Persistence directory");
        app.add_option("--master-host", master_host, "Replicate from this master host");
        app.add_option("--master-port", master_port, "Replicate from this master port")->check(CLI::Range(0U, 65535U));
        CLI11_PARSE(app, argc, argv);

        const KV::CacheStrategy selected = cache_strategy == "array"            ? KV::CacheStrategy::kArray
                                           : cache_strategy == "red-black-tree" ? KV::CacheStrategy::kRedBlackTree
                                           : cache_strategy == "skip-list"      ? KV::CacheStrategy::kSkipList
                                                                                : KV::CacheStrategy::kHash;
        const KV::EvictionPolicy eviction =
            eviction_policy == "lfu" ? KV::EvictionPolicy::kLFU : KV::EvictionPolicy::kLRU;

        KV::KVServer server(selected, capacity, persistence_directory, eviction);
        server.load_snapshot(); // restore snapshot + AOF before accepting
        if (master_port != 0)
        {
            server.attach_master(master_host, static_cast<std::uint16_t>(master_port));
        }

        std::cout << "KV server (coroutine) listening on 0.0.0.0:" << port << '\n';
        Async::detail::Engine::run(server.run(static_cast<std::uint16_t>(port)));
    }
    catch (const std::exception &ex)
    {
        std::cerr << "server error: " << ex.what() << '\n';
        return 1;
    }
    return 0;
}
