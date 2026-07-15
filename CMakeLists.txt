cmake_minimum_required(VERSION 3.20)

project(KVStore)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)

# Search for liburing on the system
pkg_check_modules(liburing REQUIRED IMPORTED_TARGET liburing)

add_executable(
	KVStore
	source/Parser.cpp
	source/Server.cpp
	source/main.cpp
)

target_link_libraries(
	KVStore
	PRIVATE
	PkgConfig::liburing
)
