cmake_minimum_required(VERSION 3.20)

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