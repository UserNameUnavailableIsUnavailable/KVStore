cmake_minimum_required(VERSION 3.20)

project(KVStore)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)

# Search for liburing on the system
pkg_check_modules(liburing REQUIRED IMPORTED_TARGET liburing)

include_directories(
	include
)

add_library(
	Utility
	source/Utility/Token.cpp
)

add_executable(
	KVServer
	source/Server/main.cpp
	source/Server/Server.cpp
	source/Server/Parser.cpp
)

target_link_libraries(
	KVServer
	PRIVATE
	PkgConfig::liburing
)

add_executable(
	KVClient
	source/Client/main.cpp
	source/Client/Client.cpp
)

target_link_libraries(
	KVClient
	PRIVATE
	Utility
)
