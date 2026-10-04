cmake_minimum_required(VERSION 3.20)

# Benchmarks are executables rather than tests: they need an RDMA device and they
# print numbers instead of asserting. Built only when KVSTORE_BUILD_BENCHMARKS is
# on, so nothing in the default build or in ctest depends on them.
add_executable(
    RdmaFileBenchmark
    RdmaFileBenchmark.cpp
)

target_link_libraries(
    RdmaFileBenchmark
    PRIVATE
    NBIO
    CLI11::CLI11
)

# The delivery layer's own end-to-end check rather than a measurement: it moves a small
# payload each way and verifies every byte of it.
add_executable(
    RdmaDeliverTest
    RdmaDeliverTest.cpp
)

target_link_libraries(
    RdmaDeliverTest
    PRIVATE
    NBIO
    CLI11::CLI11
)

add_executable(
    RdmaDeliverBenchmark
    RdmaDeliverBenchmark.cpp
)

target_link_libraries(
    RdmaDeliverBenchmark
    PRIVATE
    NBIO
    CLI11::CLI11
)
