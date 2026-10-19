cmake_minimum_required(VERSION 3.20)

project(Tutorial)

find_package(PkgConfig REQUIRED)

pkg_check_modules(RDMACM_PC REQUIRED IMPORTED_TARGET librdmacm)
pkg_check_modules(IBVERBS_PC REQUIRED IMPORTED_TARGET libibverbs)

function (add_example name)
    set(unique_name "Tutorial_RDMA${name}")
    add_executable("${unique_name}" "${name}.c")
    target_link_libraries("${unique_name}" PRIVATE
        PkgConfig::RDMACM_PC
        PkgConfig::IBVERBS_PC)
    set_target_properties("${unique_name}"
        PROPERTIES
        OUTPUT_NAME "${name}"
    )
endfunction()

add_example(Client)
add_example(Server)