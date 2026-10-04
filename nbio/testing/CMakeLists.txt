cmake_minimum_required(VERSION 3.20)

add_executable(
	nbioTesting
	BufferTesting.cpp
	ConditionTesting.cpp
	FramePoolTesting.cpp
	FileWriteChannelTesting.cpp
	SocketChannelTesting.cpp
)

find_package(GTest REQUIRED)

target_link_libraries(
	nbioTesting
	PRIVATE
	nbio
	GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(nbioTesting)