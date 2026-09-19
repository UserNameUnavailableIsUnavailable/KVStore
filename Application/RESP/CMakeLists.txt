cmake_minimum_required(VERSION 3.20)

target_sources(
    Application
    INTERFACE
    RESP.cpp
    Sender.cpp
    Receiver.cpp
)