cmake_minimum_required(VERSION 3.20)

target_sources(
    nbio
    INTERFACE
    Buffer.cpp
    BitmapMemory.cpp
    ThreadMemoryPool.cpp
)