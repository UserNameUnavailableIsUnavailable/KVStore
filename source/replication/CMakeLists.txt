add_library(kvstore_replication replication_service.cpp)
target_compile_features(kvstore_replication PUBLIC cxx_std_20)
target_include_directories(kvstore_replication PUBLIC "${CMAKE_SOURCE_DIR}/include")
target_link_libraries(kvstore_replication PUBLIC
    kvstore_backup kvstore_commands kvstore_resp nbio::nbio)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    include(GoogleTest)
    add_executable(replication_testing testing/replication_testing.cpp)
    target_include_directories(replication_testing PRIVATE "${CMAKE_SOURCE_DIR}/include")
    target_link_libraries(replication_testing PRIVATE
        kvstore_storage kvstore_backup kvstore_commands kvstore_resp nbio::nbio GTest::gtest_main)
    target_link_options(replication_testing PRIVATE "-Wl,--wrap=kvstore_write_log_v1")
    gtest_discover_tests(replication_testing)
    find_program(HOOK_OBJDUMP NAMES objdump llvm-objdump)
    if(HOOK_OBJDUMP)
        add_test(NAME logical_log_hook_binary
            COMMAND "${CMAKE_COMMAND}" "-DOBJDUMP=${HOOK_OBJDUMP}" "-DSERVER=$<TARGET_FILE:server>"
                    -P "${CMAKE_CURRENT_SOURCE_DIR}/testing/verify_hook.cmake")
    endif()
endif()
