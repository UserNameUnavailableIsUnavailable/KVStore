#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>

#include "Server.hpp"

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

		KV::Server server;
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
