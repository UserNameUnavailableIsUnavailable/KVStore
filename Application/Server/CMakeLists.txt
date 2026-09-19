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
    ConfFile.cpp
    RDMA_Transfer.cpp
    ReplicationService.cpp
    Server.cpp
    WriteHistory.cpp
)

target_link_libraries(ServerImpl INTERFACE Application)

add_executable(
    Server
    Main.cpp
)

target_link_libraries(Server PRIVATE ServerImpl CLI11::CLI11)

if(BUILD_TESTING)
	add_subdirectory(testing)
endif()