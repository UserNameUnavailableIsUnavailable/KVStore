cmake_minimum_required(VERSION 3.20)

message(STATUS "vcpkg root: $ENV{VCPKG_ROOT}")
# vcpkg toolchain must be loaded before project definition for hooks to add the package paths
set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "vcpkg toolchain file")
# put vcpkg_installed in the project root so that clean does not remove installed packages
set(VCPKG_INSTALLED_DIR "${CMAKE_SOURCE_DIR}/vcpkg_installed" CACHE PATH "vcpkg installed tree" FORCE)

project(KVStore)

# TODO: learn the difference between enable_testing() and include(CTest)
include(CTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
find_package(CLI11 CONFIG REQUIRED)

set(KV_SERVER_BACKEND "io_uring" CACHE STRING "Server backend: io_uring or epoll")
set_property(CACHE KV_SERVER_BACKEND PROPERTY STRINGS io_uring epoll)

if(KV_SERVER_BACKEND STREQUAL "io_uring")
	pkg_check_modules(liburing REQUIRED IMPORTED_TARGET liburing)
elseif(NOT KV_SERVER_BACKEND STREQUAL "epoll")
	message(FATAL_ERROR "KV_SERVER_BACKEND must be io_uring or epoll, got: ${KV_SERVER_BACKEND}")
endif()

include_directories(
	${CMAKE_SOURCE_DIR}/include
)

add_subdirectory(Common)
add_subdirectory(Client)
add_subdirectory(Server)
