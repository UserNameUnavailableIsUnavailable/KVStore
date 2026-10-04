cmake_minimum_required(VERSION 3.20)

target_sources(
    nbio
    INTERFACE
    Address.cpp
    TcpSocket.cpp
    TcpAcceptor.cpp
    TcpConnector.cpp
    TcpAcceptChannel.cpp
    TcpAcceptService.cpp
    TcpConnectChannel.cpp
    TcpConnectService.cpp
    TcpReceiveChannel.cpp
    TcpSendChannel.cpp
    TcpSessionService.cpp
    RdmaResourceManager.cpp
    RdmaConnector.cpp
    RdmaAcceptor.cpp
    RdmaAcceptChannel.cpp
    RdmaConnectChannel.cpp
    RdmaDeliverService.cpp
    RdmaReceiveChannel.cpp
    RdmaSendChannel.cpp
    RdmaSessionService.cpp
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(RDMACM_PC REQUIRED IMPORTED_TARGET librdmacm)
    pkg_check_modules(IBVERBS_PC REQUIRED IMPORTED_TARGET libibverbs)
    target_link_libraries(nbio INTERFACE fmt::fmt PkgConfig::RDMACM_PC PkgConfig::IBVERBS_PC)
endif()