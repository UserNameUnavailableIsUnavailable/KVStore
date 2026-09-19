cmake_minimum_required(VERSION 3.10)

add_executable(
    Client
    Main.cpp
)

target_link_libraries(Client PRIVATE Application)