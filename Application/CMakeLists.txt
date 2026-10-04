cmake_minimum_required(VERSION 3.20)

add_library(Application INTERFACE)

target_sources(
	Application
	INTERFACE
	Commands.cpp
)

add_subdirectory(RESP)

target_link_libraries(Application INTERFACE nbio::nbio fmt::fmt)

if(USE_JEMALLOC)
	target_link_options(Application INTERFACE
		"-Wl,--whole-archive" "${JEMALLOC_LIBRARY}" "-Wl,--no-whole-archive")
endif()

add_subdirectory(Server)
add_subdirectory(Client)