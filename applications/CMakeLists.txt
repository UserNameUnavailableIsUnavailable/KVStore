add_executable(server server.cpp)
target_link_libraries(server PRIVATE server_impl CLI11::CLI11)

if(TARGET replication_daemon_impl)
    add_executable(repld repld.cpp)
    target_link_libraries(repld PRIVATE replication_daemon_impl)
    add_dependencies(server repld)
endif()
