cmake_minimum_required(VERSION 3.20)

project(KVServer)

set(KV_SERVER_SOURCES
	source/Server.cpp
	source/main.cpp
	source/EpollServer.cpp
	source/IOUringServer.cpp
)

add_executable(
	KVServer
	${KV_SERVER_SOURCES}
)

target_link_libraries(
	KVServer
	PRIVATE
	PkgConfig::liburing
	CLI11::CLI11
	KVCommon
)

if(BUILD_TESTING)
	add_subdirectory(testing)
endif()

