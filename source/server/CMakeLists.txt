add_library(server_impl INTERFACE)
target_sources(server_impl INTERFACE server.cpp)
target_link_libraries(
    server_impl
    INTERFACE
    kvstore_backup
    kvstore_commands
    kvstore_config
    kvstore_memory
    kvstore_replication
    kvstore_resp
    kvstore_storage
    nbio::nbio
    fmt::fmt
    spdlog::spdlog
)

if(USE_JEMALLOC)
    target_link_options(server_impl INTERFACE
        "-Wl,--whole-archive" "${JEMALLOC_LIBRARY}" "-Wl,--no-whole-archive")
endif()

if(BUILD_TESTING)
    add_subdirectory(testing)
endif()