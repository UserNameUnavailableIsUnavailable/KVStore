cmake_minimum_required(VERSION 3.20)

project(KVServer)

set(KV_SERVER_SOURCES
	source/Server.cpp
	source/main.cpp
)

if(KV_SERVER_BACKEND STREQUAL "io_uring")
	list(APPEND KV_SERVER_SOURCES
		source/IOUringServer.cpp
	)
	set(KV_SERVER_BACKEND_LIBRARIES PkgConfig::liburing)
	set(KV_SERVER_BACKEND_DEFINITION KV_BACKEND_IO_URING)
elseif(KV_SERVER_BACKEND STREQUAL "epoll")
	list(APPEND KV_SERVER_SOURCES
		source/EpollServer.cpp
	)
	set(KV_SERVER_BACKEND_DEFINITION KV_BACKEND_EPOLL)
endif()

add_executable(
	KVServer
	${KV_SERVER_SOURCES}
)

target_link_libraries(
	KVServer
	PRIVATE
	${KV_SERVER_BACKEND_LIBRARIES}
	KVCommon
)

target_compile_definitions(
	KVServer
	PRIVATE
	${KV_SERVER_BACKEND_DEFINITION}
)

