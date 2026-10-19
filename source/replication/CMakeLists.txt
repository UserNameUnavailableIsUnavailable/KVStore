add_library(kvstore_replication_info replication_info.cpp repld_process.cpp)
target_compile_features(kvstore_replication_info PUBLIC cxx_std_20)
target_include_directories(kvstore_replication_info PUBLIC "${CMAKE_SOURCE_DIR}/include")
add_library(kvstore_replication replication_service.cpp)
target_compile_features(kvstore_replication PUBLIC cxx_std_20)
target_include_directories(kvstore_replication PUBLIC "${CMAKE_SOURCE_DIR}/include")
target_link_libraries(kvstore_replication PUBLIC
    kvstore_backup kvstore_commands kvstore_resp kvstore_replication_info nbio::nbio)

option(KVSTORE_BUILD_PROBE_DAEMON "Build the external libbpf write-log daemon" ON)
if(KVSTORE_BUILD_PROBE_DAEMON)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(LIBBPF REQUIRED IMPORTED_TARGET libbpf)
    find_program(BPF_CLANG clang REQUIRED)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)$")
        set(BPF_ARCH x86)
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
        set(BPF_ARCH arm64)
    else()
        message(FATAL_ERROR "Write-log uprobe supports x86_64 and aarch64")
    endif()
    set(BPF_OBJECT "${CMAKE_CURRENT_BINARY_DIR}/write_log.bpf.o")
    add_custom_command(OUTPUT "${BPF_OBJECT}"
        COMMAND "${BPF_CLANG}" -O2 -g -target bpf -D__TARGET_ARCH_${BPF_ARCH}
            -I "${CMAKE_SOURCE_DIR}/include" -I "/usr/include/${CMAKE_LIBRARY_ARCHITECTURE}"
            -c "${CMAKE_CURRENT_SOURCE_DIR}/write_log.bpf.c" -o "${BPF_OBJECT}"
        DEPENDS write_log.bpf.c "${CMAKE_SOURCE_DIR}/include/replication/probe_event.h"
        VERBATIM)
    add_custom_target(replication_probe DEPENDS "${BPF_OBJECT}")

    add_library(replication_daemon_impl INTERFACE)
    target_sources(replication_daemon_impl INTERFACE
        "${CMAKE_CURRENT_SOURCE_DIR}/replication_daemon.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/probe_capture.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/replication_transport.cpp")
    target_compile_features(replication_daemon_impl INTERFACE cxx_std_20)
    target_include_directories(replication_daemon_impl INTERFACE "${CMAKE_SOURCE_DIR}/include")
    target_link_libraries(replication_daemon_impl INTERFACE PkgConfig::LIBBPF kvstore_replication_info nbio::nbio)
    target_compile_definitions(replication_daemon_impl INTERFACE KVSTORE_BPF_OBJECT="${BPF_OBJECT}")
    add_dependencies(replication_daemon_impl replication_probe)
endif()

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    include(GoogleTest)
    add_executable(repld_process_fixture testing/repld_process_fixture.cpp)
    target_link_libraries(repld_process_fixture PRIVATE kvstore_replication_info)
    add_executable(replication_testing testing/replication_testing.cpp)
    add_dependencies(replication_testing repld_process_fixture)
    target_compile_definitions(replication_testing PRIVATE
        REPLD_PROCESS_FIXTURE="$<TARGET_FILE:repld_process_fixture>")
    target_include_directories(replication_testing PRIVATE "${CMAKE_SOURCE_DIR}/include")
    target_link_libraries(replication_testing PRIVATE kvstore_storage kvstore_replication_info GTest::gtest_main)
    target_link_options(replication_testing PRIVATE "-Wl,--wrap=kvstore_write_log_v1")
    gtest_discover_tests(replication_testing)
    add_executable(replication_daemon_testing testing/daemon_testing.cpp replication_transport.cpp)
    target_include_directories(replication_daemon_testing PRIVATE "${CMAKE_SOURCE_DIR}/include")
    target_link_libraries(replication_daemon_testing PRIVATE GTest::gtest_main kvstore_replication_info nbio::nbio)
    gtest_discover_tests(replication_daemon_testing)
    find_program(HOOK_OBJDUMP NAMES objdump llvm-objdump)
    if(HOOK_OBJDUMP)
        add_test(NAME logical_log_hook_binary
            COMMAND "${CMAKE_COMMAND}" "-DOBJDUMP=${HOOK_OBJDUMP}" "-DSERVER=$<TARGET_FILE:server>"
                    -P "${CMAKE_CURRENT_SOURCE_DIR}/testing/verify_hook.cmake")
    endif()
    find_program(NODE_EXECUTABLE node)
    if(NODE_EXECUTABLE)
        foreach(backend epoll io_uring)
            add_test(NAME replication_network_${backend}
                COMMAND "${NODE_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/testing/network_testing.cjs"
                        "$<TARGET_FILE:server>" "${backend}")
            set_tests_properties(replication_network_${backend} PROPERTIES
                SKIP_REGULAR_EXPRESSION "SKIP:" TIMEOUT 90 RUN_SERIAL TRUE)
            if(KVSTORE_BUILD_PROBE_DAEMON)
                add_test(NAME replication_probe_network_${backend}
                    COMMAND "${NODE_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/testing/network_testing.cjs"
                            "$<TARGET_FILE:server>" "${backend}" "$<TARGET_FILE:repld>" probe)
                set_tests_properties(replication_probe_network_${backend} PROPERTIES
                    SKIP_REGULAR_EXPRESSION "SKIP:" TIMEOUT 90 RUN_SERIAL TRUE)
            endif()
        endforeach()
    endif()
endif()
