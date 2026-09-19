cmake_minimum_required(VERSION 3.10)

add_executable(
	FoundationTesting
	BufferTesting.cpp
	ConditionTesting.cpp
	FramePoolTesting.cpp
	WriteChannelTesting.cpp
	SocketChannelTesting.cpp
)

find_package(GTest REQUIRED)

target_link_libraries(
	FoundationTesting
	PRIVATE
	Foundation
	GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(FoundationTesting)