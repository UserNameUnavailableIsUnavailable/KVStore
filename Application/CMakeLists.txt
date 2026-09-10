cmake_minimum_required(VERSION 3.20)

add_library(Application INTERFACE)

target_sources(
	Application
	INTERFACE
	Commands.cpp
)

add_subdirectory(RESP)

target_link_libraries(Application INTERFACE Foundation)

add_subdirectory(Server)
add_subdirectory(Client)