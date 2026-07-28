cmake_minimum_required(VERSION 3.20)

add_executable(
	KVCommonTesting

	SerializationTesting.cpp
	ParserTesting.cpp
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
