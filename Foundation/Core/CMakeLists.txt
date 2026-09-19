cmake_minimum_required(VERSION 3.10)

target_sources(
    Foundation
    INTERFACE
    Address.cpp
    File.cpp
    FileView.cpp
    Socket.cpp
    Timer.cpp
    Signal.cpp
    Notifier.cpp
    Buffer.cpp
    BitmapMemory.cpp
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Prefer the shared system libraries: providers such as siw and rxe are
    # dlopen'd from /etc/libibverbs.d, which vcpkg's static archives cannot do.
    find_library(IBVERBS_LIBRARY NAMES ibverbs
        PATHS /usr/lib/${CMAKE_LIBRARY_ARCHITECTURE} /usr/lib64 /usr/lib NO_DEFAULT_PATH)
    find_library(RDMACM_LIBRARY NAMES rdmacm
        PATHS /usr/lib/${CMAKE_LIBRARY_ARCHITECTURE} /usr/lib64 /usr/lib NO_DEFAULT_PATH)
    find_library(IBVERBS_LIBRARY NAMES ibverbs REQUIRED)
    find_library(RDMACM_LIBRARY NAMES rdmacm REQUIRED)

    target_sources(
        Foundation
        INTERFACE
        RDMA_Stream.cpp
        RDMA_Acceptor.cpp
        RDMA_Connector.cpp
    )
    target_link_libraries(
        Foundation
        INTERFACE
        ${RDMACM_LIBRARY}
        ${IBVERBS_LIBRARY}
    )
endif()
