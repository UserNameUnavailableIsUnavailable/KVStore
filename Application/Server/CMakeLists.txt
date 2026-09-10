cmake_minimum_required(VERSION 3.20)

add_library(
    ServerImpl
    INTERFACE
)
target_sources(
    ServerImpl
    INTERFACE
    AppendOnlyFile.cpp
    Backup.cpp
    Server.cpp
)

target_link_libraries(ServerImpl INTERFACE Application)

add_executable(
    Server
    Main.cpp
)

target_link_libraries(Server PRIVATE ServerImpl)

if(BUILD_TESTING)
	add_subdirectory(testing)
endif()