add_library(kvstore_memory STATIC
    memory_pool.cpp
    buffer.cpp
)
target_compile_features(kvstore_memory PUBLIC cxx_std_20)
target_link_libraries(kvstore_memory PRIVATE nbio::nbio)