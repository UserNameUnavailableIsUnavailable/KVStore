cmake_minimum_required(VERSION 3.20)

project(KVCommon)

add_library(
	KVCommon
	STATIC
	Token.cpp
	Protocol.cpp
	Task.cpp
	MemoryResource.cpp
)

target_link_libraries(
	KVCommon
	PRIVATE
	${CMAKE_DL_LIBS}
)
