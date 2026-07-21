cmake_minimum_required(VERSION 3.20)

project(KVServer)

add_executable(
	KVServer
	source/main.cpp
	source/Server.cpp
)

target_link_libraries(
	KVServer
	PRIVATE
	PkgConfig::liburing
	KVCommon
)

