add_library(kvstore_storage append_only_file.cpp write_log.cpp)
target_compile_features(kvstore_storage PUBLIC cxx_std_20)
target_include_directories(kvstore_storage PUBLIC "${CMAKE_SOURCE_DIR}/include")
target_link_libraries(kvstore_storage PUBLIC kvstore_checksum kvstore_commands kvstore_resp nbio::nbio)