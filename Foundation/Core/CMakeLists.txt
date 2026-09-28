cmake_minimum_required(VERSION 3.20)

target_sources(
    Foundation
    INTERFACE
    SocketAddress.cpp
    File.cpp
    FileView.cpp
    TcpSocket.cpp
    TcpAcceptor.cpp
    TcpConnector.cpp
    SystemTimer.cpp
    SystemSignal.cpp
    EventNotifier.cpp
    Buffer.cpp
    BitmapMemory.cpp
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")

    find_package(PkgConfig REQUIRED)

    pkg_check_modules(RDMACM_PC REQUIRED IMPORTED_TARGET librdmacm)
    pkg_check_modules(IBVERBS_PC REQUIRED IMPORTED_TARGET libibverbs)

    target_sources(Foundation
        INTERFACE
            RdmaResourceManager.cpp
            RdmaAcceptor.cpp
            RdmaConnector.cpp)

    target_link_libraries(Foundation
        INTERFACE
            PkgConfig::RDMACM_PC
            PkgConfig::IBVERBS_PC)
endif()
