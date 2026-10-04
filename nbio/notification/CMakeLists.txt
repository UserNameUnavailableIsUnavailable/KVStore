cmake_minimum_required(VERSION 3.20)

target_sources(
    nbio
    INTERFACE
    EventNotifier.cpp
    ConditionVariable.cpp
    EventNotifyChannel.cpp
)
