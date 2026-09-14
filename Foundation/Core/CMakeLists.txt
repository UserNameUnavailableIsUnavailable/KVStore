cmake_minimum_required(VERSION 3.20)

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
    RDMA_Channel.cpp
)
