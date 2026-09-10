cmake_minimum_required(VERSION 3.20)

add_library(Foundation INTERFACE)

target_sources(
    Foundation
    INTERFACE
    Address.cpp
    Socket.cpp
    Timer.cpp
    Signal.cpp
    Notifier.cpp
    Buffer.cpp
)

add_subdirectory(Async)

add_subdirectory(examples)

if(BUILD_TESTING)
    add_subdirectory(testing)
endif()
