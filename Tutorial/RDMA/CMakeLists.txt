cmake_minimum_required(VERSION 3.20)

project(Tutorial)

find_package(PkgConfig REQUIRED)
pkg_check_modules(rdma REQUIRED IMPORTED_TARGET librdmacm libibverbs)

function (add_example name)
    set(unique_name "Tutorial_RDMA${name}")
    add_executable("${unique_name}" "${name}.c")
    target_link_libraries("${unique_name}" PRIVATE PkgConfig::rdma)
    set_target_properties("${unique_name}"
        PROPERTIES
        OUTPUT_NAME "${name}"
    )
endfunction()

add_example(Client)
add_example(Server)