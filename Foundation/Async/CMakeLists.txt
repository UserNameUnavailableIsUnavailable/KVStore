cmake_minimum_required(VERSION 3.20)

# Core Async is backend-agnostic: coroutine machinery, the scheduler, and the
# runtime-tag-driven entry points. Channels, multiplexers and the concrete
# runtimes live in NBIO and RDMA.
target_sources(
    Foundation
    INTERFACE
    FramePool.cpp
    Scheduler.cpp
)
