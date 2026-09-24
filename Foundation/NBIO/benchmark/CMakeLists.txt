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
    Foundation
    CLI11::CLI11
)
