add_library(kvstore_backup backup.cpp)
target_compile_features(kvstore_backup PUBLIC cxx_std_20)
target_include_directories(kvstore_backup PUBLIC "${CMAKE_SOURCE_DIR}/include")
target_link_libraries(kvstore_backup PUBLIC kvstore_checksum kvstore_storage)