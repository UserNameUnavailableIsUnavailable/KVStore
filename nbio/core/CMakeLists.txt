cmake_minimum_required(VERSION 3.20)

target_sources(
    nbio
    INTERFACE
    EpollMultiplexer.cpp
    URingMultiplexer.cpp
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")

    find_package(PkgConfig REQUIRED)

    pkg_check_modules(URING_PC REQUIRED IMPORTED_TARGET liburing)
    target_link_libraries(nbio
        INTERFACE
            PkgConfig::URING_PC)
endif()
