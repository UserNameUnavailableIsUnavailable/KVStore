cmake_minimum_required(VERSION 3.20)

# Coroutine-based KV server with persistence (SAVE + AOF) and replication.
add_executable(
    KVserver
    main.cpp
    KVServer.cpp
    Persistence.cpp
)

find_package(CLI11 CONFIG REQUIRED)

target_include_directories(KVserver PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
    KVserver
    PRIVATE
    Foundation
    Common
    CLI11::CLI11
)
