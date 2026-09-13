cmake_minimum_required(VERSION 3.20)

find_package(PkgConfig REQUIRED)
pkg_check_modules(liburing REQUIRED IMPORTED_TARGET liburing)

find_package(spdlog REQUIRED)

target_sources(
    Foundation
    INTERFACE
    Engine.cpp
    EpollMultiplexer.cpp
    ConditionVariable.cpp
    FileStream.cpp
    ListenChannel.cpp
    NBIO.cpp
    NotifyChannel.cpp
    ReadChannel.cpp
    ReceiveChannel.cpp
    SendChannel.cpp
    Session.cpp
    SignalChannel.cpp
    TimerChannel.cpp
    URingMultiplexer.cpp
    WriteChannel.cpp
)

target_link_libraries(
    Foundation
    INTERFACE
    PkgConfig::liburing
    spdlog::spdlog
)
