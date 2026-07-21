cmake_minimum_required(VERSION 3.20)

project(KVClient)

add_executable(
	KVClient
	source/main.cpp
	source/Client.cpp
)

target_link_libraries(
	KVClient
	PRIVATE
	KVCommon
)

