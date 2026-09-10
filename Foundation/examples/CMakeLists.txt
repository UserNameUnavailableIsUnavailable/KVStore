cmake_minimum_required(VERSION 3.20)

function(add_example NAME)
    add_executable(
        ${NAME}
        ${NAME}.cpp
    )
    target_link_libraries(
        ${NAME}
        PRIVATE
        Foundation
    )
endfunction()

add_example(Echo)
add_example(Sleep)
add_example(Grace)
add_example(ScopedSignalService)
add_example(Condition)