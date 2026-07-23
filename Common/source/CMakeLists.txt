cmake_minimum_required(VERSION 3.20)

project(KVCommon)

add_library(
	KVCommon
	STATIC
	Token.cpp
	Command.cpp
	Result.cpp
)
