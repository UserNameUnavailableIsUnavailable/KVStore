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

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
find_package(CLI11 CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

include_directories(
	SYSTEM
	${CMAKE_SOURCE_DIR}
	${CMAKE_SOURCE_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include
)

add_subdirectory(Foundation)
add_subdirectory(Application)

