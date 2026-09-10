cmake_minimum_required(VERSION 3.20)

project(KVCommon)

add_library(
	KVCommon
	STATIC
	Token.cpp
	Protocol.cpp
	Task.cpp
)

add_subdirectory(testing)
add_subdirectory(benchmark)
