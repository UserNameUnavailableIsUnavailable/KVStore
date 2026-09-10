cmake_minimum_required(VERSION 3.20)

add_executable(
	KVCommonTesting

	DataStoreTesting.cpp
	SkipListTesting.cpp
	SocketTesting.cpp
	SlabTesting.cpp
	SessionManagerTesting.cpp
	TimerTesting.cpp
	CoroutineTest.cpp
)

find_package(GTest REQUIRED)

target_link_libraries(
	KVCommonTesting

	PRIVATE
	KVCommon
	GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(KVCommonTesting)

add_executable(
	KVProtocolTesting
	ProtocolTesting.cpp
)

target_link_libraries(
	KVProtocolTesting
	PRIVATE
	KVCommon
	GTest::gtest_main
)

gtest_discover_tests(KVProtocolTesting)
