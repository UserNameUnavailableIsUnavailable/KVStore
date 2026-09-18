add_library(Foundation INTERFACE)

if(KVSTORE_USE_JEMALLOC)
    # INTERFACE, not PRIVATE: the allocator has to reach the process that
    # allocates, and the target that links Foundation is that process.
    target_link_options(Foundation INTERFACE
        "-Wl,--whole-archive" "${JEMALLOC_LIBRARY}" "-Wl,--no-whole-archive")
endif()

add_subdirectory(Core)
add_subdirectory(Async)
add_subdirectory(NBIO)
add_subdirectory(examples)

if(BUILD_TESTING)
    add_subdirectory(testing)
endif()