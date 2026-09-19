cmake_minimum_required(VERSION 3.20)

project(Guides)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_subdirectory(RDMA)
endif()