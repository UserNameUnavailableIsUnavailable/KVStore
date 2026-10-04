cmake_minimum_required(VERSION 3.20)

target_sources(
    NBIO
    INTERFACE
    SocketAddress.cpp
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
)
