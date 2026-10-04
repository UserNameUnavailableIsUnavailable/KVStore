cmake_minimum_required(VERSION 3.20)

target_sources(
    nbio
    INTERFACE
    SystemTimer.cpp
    SystemTimerChannel.cpp
)
