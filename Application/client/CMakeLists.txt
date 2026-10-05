cmake_minimum_required(VERSION 3.20)

add_executable(
    client
    main.cpp
)

target_link_libraries(client PRIVATE Application)