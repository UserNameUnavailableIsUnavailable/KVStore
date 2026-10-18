add_library(kvstore_resp
    decoder.cpp
    encoder.cpp
    receiver.cpp
    resp.cpp
    sender.cpp
)
target_compile_features(kvstore_resp PUBLIC cxx_std_20)
target_include_directories(kvstore_resp PUBLIC "${CMAKE_SOURCE_DIR}/include")
target_link_libraries(kvstore_resp PUBLIC nbio::nbio)

if(BUILD_TESTING)
    find_package(GTest CONFIG REQUIRED)
    add_executable(resp_testing testing.cpp)
    target_link_libraries(resp_testing PRIVATE kvstore_resp GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(resp_testing)
endif()