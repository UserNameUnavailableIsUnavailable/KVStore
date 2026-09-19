cmake_minimum_required(VERSION 3.10)

function(add_example name)
    add_executable(
        ${name}
        ${name}.cpp
    )
    target_link_libraries(
        ${name}
        PRIVATE
        Foundation
    )
endfunction()

add_example(Echo)
add_example(Sleep)
add_example(Grace)
add_example(ScopedSignalService)
add_example(Condition)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_example(RDMA_Echo)
    add_example(RDMA_AsyncEcho)
endif()