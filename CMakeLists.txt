cmake_minimum_required(VERSION 3.20)

message(STATUS "vcpkg root: $ENV{VCPKG_ROOT}")
# vcpkg toolchain must be loaded before project definition for hooks to add the package paths
set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "vcpkg toolchain file")
# put vcpkg_installed in the project root so that clean does not remove installed packages
set(VCPKG_INSTALLED_DIR "${CMAKE_SOURCE_DIR}/vcpkg_installed" CACHE PATH "vcpkg installed tree" FORCE)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(KVStore)
# TODO: learn the difference between enable_testing() and include(CTest)
include(CTest)

set(CMAKE_C_STANDARD 23)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# if (MSVC)
#     add_compile_options(/W4)
# else()
#     add_compile_options(-Wall -Wextra -Wpedantic)
# endif()

find_package(CLI11 CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(nbio CONFIG REQUIRED)

# jemalloc can replace the process allocator: its archive defines malloc/free/
# realloc and the C++ operator new/delete, so it is force-linked -- whole
# archive -- because otherwise the linker would satisfy those from libc and the
# archive would contribute nothing.
#
option(USE_JEMALLOC "Link jemalloc as the process allocator" OFF)
if(USE_JEMALLOC)
    find_library(JEMALLOC_LIBRARY NAMES jemalloc REQUIRED)
endif()

option(CUSTOM_MEMORY_POOLING "Use custom per-thread global new/delete pool" OFF)
if(CUSTOM_MEMORY_POOLING AND USE_JEMALLOC)
	message(FATAL_ERROR "CUSTOM_MEMORY_POOLING and USE_JEMALLOC are mutually exclusive")
endif()

# The C++ benchmarks measure throughput against real hardware, so they are built
# only when asked for: nothing in the default build or in ctest needs them.
option(KVSTORE_BUILD_BENCHMARKS "Build the throughput benchmarks" ON)

include_directories(
	SYSTEM
	${CMAKE_SOURCE_DIR}
	${CMAKE_SOURCE_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include
)

add_subdirectory(Tutorial)
add_subdirectory(Application)

set(FIND_TARGET "" CACHE STRING "Build target whose file path FindTarge prints")
if(FIND_TARGET)
	if(NOT TARGET "${FIND_TARGET}")
		message(FATAL_ERROR "FIND_TARGET names an unknown target: ${FIND_TARGET}")
	endif()
	add_custom_target(FindTarget
		COMMAND ${CMAKE_COMMAND} -E echo "TARGET_FILE=$<TARGET_FILE:${FIND_TARGET}>"
		DEPENDS "${FIND_TARGET}"
		VERBATIM
	)
else()
	add_custom_target(FindTarget
		COMMAND ${CMAKE_COMMAND} -E echo "Set FIND_TARGET to the target whose file path should be printed"
		VERBATIM
	)
endif()
