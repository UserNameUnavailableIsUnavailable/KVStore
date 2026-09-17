cmake_minimum_required(VERSION 3.20)

add_executable(
	ServerTesting
	BackupTesting.cpp
	AppendOnlyFileTesting.cpp
	RDMA_TransferTesting.cpp
	ReplicationServiceTesting.cpp
	WriteHistoryTesting.cpp
	ConfFileTesting.cpp
)

find_package(GTest REQUIRED)

target_link_libraries(
	ServerTesting
	PRIVATE
	ServerImpl
	GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(ServerTesting)