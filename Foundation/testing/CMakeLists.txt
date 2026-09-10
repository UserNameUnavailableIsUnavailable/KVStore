cmake_minimum_required(VERSION 3.20)

add_executable(
	FoundationTesting
	ConditionTesting.cpp
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