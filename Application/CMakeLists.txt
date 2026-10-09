cmake_minimum_required(VERSION 3.20)

add_library(Application INTERFACE)

target_sources(
	Application
	INTERFACE
	commands.cpp
	memory_pool.cpp
)

add_subdirectory(resp)

target_link_libraries(Application INTERFACE nbio::nbio fmt::fmt)

if(USE_JEMALLOC)
	target_link_options(Application INTERFACE
		"-Wl,--whole-archive" "${JEMALLOC_LIBRARY}" "-Wl,--no-whole-archive")
endif()

add_subdirectory(server)
add_subdirectory(client)