add_library(kvstore_config conf_file.cpp)
target_compile_features(kvstore_config PUBLIC cxx_std_20)
target_include_directories(kvstore_config PUBLIC "${CMAKE_SOURCE_DIR}/include")
target_link_libraries(kvstore_config PUBLIC kvstore_commands)