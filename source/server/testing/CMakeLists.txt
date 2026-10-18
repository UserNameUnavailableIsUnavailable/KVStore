cmake_minimum_required(VERSION 3.20)

add_executable(
	server_testing
	backup_testing.cpp
	conf_file_testing.cpp
	encoder_testing.cpp
	scanner_testing.cpp
	sender_testing.cpp
	resp_testing.cpp
	store_testing.cpp
)

add_executable(eviction_policy_testing eviction_policy_testing.cpp)

find_package(GTest REQUIRED)

target_link_libraries(
	server_testing
	PRIVATE
	server_impl
	kvstore_memory
	GTest::gtest_main
)

target_link_libraries(eviction_policy_testing PRIVATE kvstore_commands kvstore_storage GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(server_testing)
gtest_discover_tests(eviction_policy_testing)
