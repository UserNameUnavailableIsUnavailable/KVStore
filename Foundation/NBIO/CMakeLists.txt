cmake_minimum_required(VERSION 3.20)

# The vcpkg liburing port ships no CMake config package, only pkg-config
# (.pc) files, so pkg-config is the way to consume it. vcpkg's toolchain adds
# its installed tree to CMAKE_PREFIX_PATH, which FindPkgConfig turns into
# PKG_CONFIG_PATH, so this resolves to the vcpkg-managed liburing.
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
    TcpAcceptChannel.cpp
    NBIO.cpp
    EventNotifyChannel.cpp
    RdmaAcceptChannel.cpp
    RdmaConnectChannel.cpp
    RdmaReceiveChannel.cpp
    RdmaSendChannel.cpp
    RdmaSession.cpp
    FileReadChannel.cpp
    TcpReceiveChannel.cpp
    TcpSendChannel.cpp
    TcpSession.cpp
    SystemSignalChannel.cpp
    SystemTimerChannel.cpp
    URingMultiplexer.cpp
    FileWriteChannel.cpp
)

target_link_libraries(
    Foundation
    INTERFACE
    PkgConfig::liburing
    spdlog::spdlog
)

if(KVSTORE_BUILD_BENCHMARKS)
    add_subdirectory(benchmark)
endif()
