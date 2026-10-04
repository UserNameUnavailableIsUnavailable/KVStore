add_library(nbio INTERFACE)

if(CUSTOM_MEMORY_POOLING)
    target_compile_definitions(nbio INTERFACE CUSTOM_MEMORY_POOLING=1)
endif()

if(CUSTOM_MEMORY_POOLING AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_options(nbio INTERFACE
        "-Wl,--wrap=malloc"
        "-Wl,--wrap=calloc"
        "-Wl,--wrap=realloc"
        "-Wl,--wrap=free")
endif()

if(USE_JEMALLOC)
    # INTERFACE, not PRIVATE: the allocator has to reach the process that
    # allocates, and the target that links nbio is that process.
    target_link_options(nbio INTERFACE
        "-Wl,--whole-archive" "${JEMALLOC_LIBRARY}" "-Wl,--no-whole-archive")
endif()

add_subdirectory(core)
add_subdirectory(async)
add_subdirectory(runtime)
add_subdirectory(utility)
add_subdirectory(fs)
add_subdirectory(net)
add_subdirectory(notification)
add_subdirectory(signal)
add_subdirectory(time)
add_subdirectory(examples)

if(KVSTORE_BUILD_BENCHMARKS)
    add_subdirectory(benchmark)
endif()

if(BUILD_TESTING)
    add_subdirectory(testing)
endif()