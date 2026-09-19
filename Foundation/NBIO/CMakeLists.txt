cmake_minimum_required(VERSION 3.10)

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
    AcceptChannel.cpp
    NBIO.cpp
    NotifyChannel.cpp
    RDMA_AcceptChannel.cpp
    RDMA_ConnectChannel.cpp
    RDMA_ReceiveChannel.cpp
    RDMA_SendChannel.cpp
    RDMA_Session.cpp
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
