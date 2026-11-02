add_executable(server server.cpp)
target_link_libraries(server PRIVATE server_impl CLI11::CLI11)
