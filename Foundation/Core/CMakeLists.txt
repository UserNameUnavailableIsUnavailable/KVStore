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
    # Prefer the shared system libraries: providers such as siw and rxe are
    # dlopen'd from /etc/libibverbs.d, which vcpkg's static archives cannot do.
    find_library(IBVERBS_LIBRARY NAMES ibverbs
        PATHS /usr/lib/${CMAKE_LIBRARY_ARCHITECTURE} /usr/lib64 /usr/lib NO_DEFAULT_PATH)
    find_library(RDMA_CM_LIBRARY NAMES rdmacm
        PATHS /usr/lib/${CMAKE_LIBRARY_ARCHITECTURE} /usr/lib64 /usr/lib NO_DEFAULT_PATH)
    find_library(IBVERBS_LIBRARY NAMES ibverbs REQUIRED)
    find_library(RDMA_CM_LIBRARY NAMES rdmacm REQUIRED)

    target_sources(
        Foundation
        INTERFACE
        RdmaResourceManager.cpp
        RdmaAcceptor.cpp
        RdmaConnector.cpp
    )
    target_link_libraries(
        Foundation
        INTERFACE
        ${RDMA_CM_LIBRARY}
        ${IBVERBS_LIBRARY}
    )
endif()
