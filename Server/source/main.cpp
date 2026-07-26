#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>

#ifdef __linux__
#if defined(KV_BACKEND_IO_URING) && defined(KV_BACKEND_EPOLL)
#error "Only one server backend may be enabled"
#elif defined(KV_BACKEND_IO_URING)
#include "Server/IOUringServer.hpp"
using ServerImplementation = KV::IOUringServer;
#elif defined(KV_BACKEND_EPOLL)
#include "Server/EpollServer.hpp"
using ServerImplementation = KV::EpollServer;
#else
#error "A server backend must be enabled"
#endif
#else
#error "This server implementation is only supported on Linux"
#endif

int main(int argc, char** argv)
{
	try
	{
		std::uint16_t port = 8080;
		if (argc > 1)
		{
			auto parsed = std::strtoul(argv[1], nullptr, 10);
			if (parsed == 0 || parsed > 65535)
			{
				std::cerr << "invalid port: " << argv[1] << '\n';
				return 1;
			}
			port = static_cast<std::uint16_t>(parsed);
		}

		ServerImplementation server;
		server.Bind(port);
		std::cout << "KV server listening on 0.0.0.0:" << port << '\n';
		server.Run();
	}
	catch (const std::exception& ex)
	{
		std::cerr << "server error: " << ex.what() << '\n';
		return 1;
	}

	return 0;
}
