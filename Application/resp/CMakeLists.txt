cmake_minimum_required(VERSION 3.20)

target_sources(
    Application
    INTERFACE
    resp.cpp
    sender.cpp
    receiver.cpp
)