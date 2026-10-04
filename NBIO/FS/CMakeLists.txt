cmake_minimum_required(VERSION 3.20)

target_sources(
    NBIO
    INTERFACE
    File.cpp
    FileView.cpp
    FileReadChannel.cpp
    FileStream.cpp
    FileStreamService.cpp
    FileWriteChannel.cpp
)
