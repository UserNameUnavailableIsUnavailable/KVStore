cmake_minimum_required(VERSION 3.20)

find_package(PkgConfig REQUIRED)
pkg_check_modules(liburing REQUIRED IMPORTED_TARGET liburing)

find_package(spdlog REQUIRED)

target_sources(
    Foundation
    INTERFACE
	Async.cpp
	Engine.cpp
	EpollMultiplexer.cpp
	ListenChannel.cpp
	ListenService.cpp
	ReceiveChannel.cpp
	Scheduler.cpp
	SendChannel.cpp
	Session.cpp
	SignalChannel.cpp
	TimerChannel.cpp
	TimerService.cpp
	URingMultiplexer.cpp
	NotifyChannel.cpp
	Condition.cpp
	NotifyService.cpp
)

target_link_libraries(
	Foundation
	INTERFACE
	PkgConfig::liburing
	spdlog::spdlog
)