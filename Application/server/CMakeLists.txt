cmake_minimum_required(VERSION 3.20)

add_library(
    server_impl
    INTERFACE
)
target_sources(
    server_impl
    INTERFACE
    append_only_file.cpp
    backup.cpp
    conf_file.cpp
    replication_service.cpp
    server.cpp
)

target_link_libraries(server_impl INTERFACE Application)

add_executable(
    server
    main.cpp
)

target_link_libraries(server PRIVATE server_impl CLI11::CLI11)

if(BUILD_TESTING)
	add_subdirectory(testing)
endif()