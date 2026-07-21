cmake_minimum_required(VERSION 3.20)

project(KVCommon)

add_library(
	KVCommon STATIC
	source/Token.cpp
	source/LRUCache.cpp
	source/Parser.cpp
)