cmake_minimum_required(VERSION 3.20)

add_executable(
	ApplicationBackupTesting
	BackupTesting.cpp
	AppendOnlyFileTesting.cpp
)

find_package(GTest REQUIRED)

target_link_libraries(
	ApplicationBackupTesting
	PRIVATE
	ServerImpl
	GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(ApplicationBackupTesting)