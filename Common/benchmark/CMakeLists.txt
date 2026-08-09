add_executable(
	KVSkipListBenchmark
	SkipListBenchmark.cpp
)

find_package(benchmark CONFIG REQUIRED)

target_link_libraries(
	KVSkipListBenchmark
	PRIVATE
	benchmark::benchmark
)
