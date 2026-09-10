#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <vector>

#include <CLI/CLI.hpp>

#include "Server/EpollServer.hpp"
#include "Server/IOUringServer.hpp"
#include "Server/Server.hpp"

int main(int argc, char** argv)
{
	try
	{
		unsigned int port = 8080;
		std::string networking_model = "epoll";
		std::string cache_strategy = "hash";
		std::string persistent_directory = "persistent";
		std::string allocator = "default";
		bool enable_memory_pooling = false;
		std::size_t memory_pool_size = 2ULL * 1024ULL * 1024ULL * 1024ULL;
		CLI::App app("KVStore server");
		app.add_option("-p,--port", port, "TCP port to bind")
			->check(CLI::Range(1U, 65535U));
		app.add_option("-n,--networking-model", networking_model, "I/O backend")
			->check(CLI::IsMember({"io_uring", "epoll"}));
		app.add_option("-c,--cache-strategy", cache_strategy, "Index implementation")
			->check(CLI::IsMember({"hash", "array", "red-black-tree", "skip-list"}));
		app.add_option("--persistent-dir", persistent_directory, "Persistence directory");
		// Which malloc backs the process is chosen outside the program, by linking
		// or LD_PRELOAD, so it is deliberately not an option here.  This selects
		// the pmr strategy layered on top of it.
		app.add_option("--allocator", allocator, "Allocation strategy")
			->check(CLI::IsMember({"default", "pool"}));
		app.add_flag("--memory-pooling", enable_memory_pooling,
			"Use an unsynchronized PMR pool backed by a preallocated block");
		app.add_option("--memory-pool-size", memory_pool_size,
			"Memory-pool backing size in bytes (default: 2147483648)")
			->check(CLI::PositiveNumber);
		CLI11_PARSE(app, argc, argv);
		if (enable_memory_pooling)
		{
			allocator = "pool";
		}

		const KV::CacheStrategy selected_cache =
			cache_strategy == "array" ? KV::CacheStrategy::kArray :
			cache_strategy == "red-black-tree" ? KV::CacheStrategy::kRedBlackTree :
			cache_strategy == "skip-list" ? KV::CacheStrategy::kSkipList :
			KV::CacheStrategy::kHash;
		std::vector<std::byte> memory_pool_buffer;
		std::unique_ptr<std::pmr::monotonic_buffer_resource> pool_backing;
		std::unique_ptr<std::pmr::unsynchronized_pool_resource> memory_pool;
		// The default resource routes to operator new, hence to whatever malloc
		// the process was linked or preloaded against.  Selecting "pool" instead
		// takes one large block from that same malloc and sub-allocates within it.
		std::pmr::memory_resource* resource = std::pmr::get_default_resource();
		if (allocator == "pool")
		{
			memory_pool_buffer.resize(memory_pool_size);
			pool_backing = std::make_unique<std::pmr::monotonic_buffer_resource>(
				memory_pool_buffer.data(), memory_pool_buffer.size(), std::pmr::null_memory_resource());
			memory_pool = std::make_unique<std::pmr::unsynchronized_pool_resource>(pool_backing.get());
			resource = memory_pool.get();
		}

		std::unique_ptr<KV::Server> server;
		if (networking_model == "epoll")
		{
			server = std::make_unique<KV::EpollServer>(selected_cache, persistent_directory, resource);
		}
		else
		{
			server = std::make_unique<KV::IOUringServer>(selected_cache, persistent_directory, resource);
		}
		std::cout << "KV server listening on 0.0.0.0:" << port << '\n';
		server->run(static_cast<std::uint16_t>(port));
	}
	catch (const std::exception& ex)
	{
		std::cerr << "server error: " << ex.what() << '\n';
		return 1;
	}

	return 0;
}
