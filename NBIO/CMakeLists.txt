add_library(NBIO INTERFACE)

if(CUSTOM_MEMORY_POOLING)
    target_compile_definitions(NBIO INTERFACE CUSTOM_MEMORY_POOLING=1)
endif()

if(CUSTOM_MEMORY_POOLING AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_options(NBIO INTERFACE
        "-Wl,--wrap=malloc"
        "-Wl,--wrap=calloc"
        "-Wl,--wrap=realloc"
        "-Wl,--wrap=free")
endif()

if(USE_JEMALLOC)
    # INTERFACE, not PRIVATE: the allocator has to reach the process that
    # allocates, and the target that links NBIO is that process.
    target_link_options(NBIO INTERFACE
        "-Wl,--whole-archive" "${JEMALLOC_LIBRARY}" "-Wl,--no-whole-archive")
endif()

add_subdirectory(Core)
add_subdirectory(Async)
add_subdirectory(Runtime)
add_subdirectory(Utility)
add_subdirectory(FS)
add_subdirectory(Net)
add_subdirectory(RDMA)
add_subdirectory(Notification)
add_subdirectory(Signal)
add_subdirectory(Time)
add_subdirectory(examples)

if(KVSTORE_BUILD_BENCHMARKS)
    add_subdirectory(benchmark)
endif()

if(BUILD_TESTING)
    add_subdirectory(testing)
endif()