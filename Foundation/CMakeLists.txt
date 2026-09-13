add_library(Foundation INTERFACE)

add_subdirectory(Core)
add_subdirectory(Async)
add_subdirectory(NBIO)
add_subdirectory(examples)

if(BUILD_TESTING)
    add_subdirectory(testing)
endif()