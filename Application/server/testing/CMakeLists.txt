cmake_minimum_required(VERSION 3.20)

add_executable(
	server_testing
	backup_testing.cpp
	append_only_file_testing.cpp
	conf_file_testing.cpp
	encoder_testing.cpp
	scanner_testing.cpp
	resp_testing.cpp
)

find_package(GTest REQUIRED)

target_link_libraries(
	server_testing
	PRIVATE
	server_impl
	GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(server_testing)