cmake_minimum_required(VERSION 3.20)

project(Tutorial)

find_package(PkgConfig REQUIRED)
pkg_check_modules(rdma REQUIRED IMPORTED_TARGET librdmacm libibverbs)

function (add_example name)
    add_executable(${name} ${name}.c)
    target_link_libraries(${name} PRIVATE PkgConfig::rdma)
endfunction()

add_example(Client)
add_example(Server)