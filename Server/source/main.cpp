#include <cstdint>
#include <cerrno>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <system_error>
#include <vector>

#include <unistd.h>

#include <CLI/CLI.hpp>

#include "Common/MemoryResource.hpp"
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
		app.add_option("--allocator", allocator, "Allocator implementation")
			->check(CLI::IsMember({"default", "pool", "jemalloc"}));
		app.add_flag("--memory-pooling", enable_memory_pooling,
			"Use an unsynchronized PMR pool backed by a preallocated block");
		app.add_option("--memory-pool-size", memory_pool_size,
			"Memory-pool backing size in bytes (default: 2147483648)")
			->check(CLI::PositiveNumber);
		CLI11_PARSE(app, argc, argv);
		if (enable_memory_pooling)
		{
			if (allocator != "default" && allocator != "pool")
			{
				throw CLI::ValidationError("--memory-pooling conflicts with --allocator " + allocator);
			}
			allocator = "pool";
		}
		if (allocator == "jemalloc" && std::getenv("KVSTORE_JEMALLOC_PRELOADED") == nullptr)
		{
			const char* existing_preload = std::getenv("LD_PRELOAD");
			std::string preload = "libjemalloc.so.2";
			if (existing_preload != nullptr && existing_preload[0] != '\0')
			{
				preload += ':';
				preload += existing_preload;
			}
			::setenv("KVSTORE_JEMALLOC_PRELOADED", "1", 1);
			::setenv("LD_PRELOAD", preload.c_str(), 1);
			::execv("/proc/self/exe", argv);
			throw std::system_error(errno, std::generic_category(), "failed to restart with jemalloc");
		}

		const KV::CacheStrategy selected_cache =
			cache_strategy == "array" ? KV::CacheStrategy::kArray :
			cache_strategy == "red-black-tree" ? KV::CacheStrategy::kRedBlackTree :
			cache_strategy == "skip-list" ? KV::CacheStrategy::kSkipList :
			KV::CacheStrategy::kHash;
		std::vector<std::byte> memory_pool_buffer;
		std::unique_ptr<std::pmr::monotonic_buffer_resource> pool_backing;
		std::unique_ptr<std::pmr::unsynchronized_pool_resource> memory_pool;
		KV::JemallocMemoryResource jemalloc_resource;
		std::pmr::memory_resource* resource = std::pmr::get_default_resource();
		if (allocator == "pool")
		{
			memory_pool_buffer.resize(memory_pool_size);
			pool_backing = std::make_unique<std::pmr::monotonic_buffer_resource>(
				memory_pool_buffer.data(), memory_pool_buffer.size(), std::pmr::null_memory_resource());
			memory_pool = std::make_unique<std::pmr::unsynchronized_pool_resource>(pool_backing.get());
			resource = memory_pool.get();
		}
		else if (allocator == "jemalloc")
		{
			resource = &jemalloc_resource;
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
		server->Run(static_cast<std::uint16_t>(port));
	}
	catch (const std::exception& ex)
	{
		std::cerr << "server error: " << ex.what() << '\n';
		return 1;
	}

	return 0;
}
