add_library(kvstore_commands ../commands.cpp)
target_compile_features(kvstore_commands PUBLIC cxx_std_20)
target_include_directories(kvstore_commands PUBLIC "${CMAKE_SOURCE_DIR}/include")
target_link_libraries(kvstore_commands PUBLIC kvstore_resp nbio::nbio)