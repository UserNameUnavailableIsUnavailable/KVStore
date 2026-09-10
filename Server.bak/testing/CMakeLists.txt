find_package(GTest REQUIRED)

add_executable(
	KVServerTesting
	ServerTesting.cpp
	Foundation/source/Server.cpp
)

target_link_libraries(
	KVServerTesting
	PRIVATE
	KVCommon
	GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(KVServerTesting)