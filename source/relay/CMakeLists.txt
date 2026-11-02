# The relay daemon: a passive eBPF tap of the master's outbound push stream that
# broadcasts the extracted payload to connected slaves, with a TCP-replica
# fallback when BPF cannot be loaded. libbpf and clang are optional -- without
# them the daemon builds and works in fallback mode only.

find_package(PkgConfig QUIET)
set(RELAY_BPF_OBJECT "")

if(PkgConfig_FOUND)
	pkg_check_modules(LIBBPF IMPORTED_TARGET libbpf)
endif()

if(LIBBPF_FOUND)
	find_program(RELAY_BPF_CLANG NAMES clang)
	# bpf/bpf_helpers.h ships with libbpf-dev alongside libbpf.h.
	find_path(RELAY_BPF_INCLUDE_DIR bpf/bpf_helpers.h)
endif()

if(LIBBPF_FOUND AND RELAY_BPF_CLANG AND RELAY_BPF_INCLUDE_DIR)
	set(RELAY_BPF_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/relay.bpf.c")
	set(RELAY_BPF_OBJECT "${CMAKE_CURRENT_BINARY_DIR}/relay.bpf.o")
	add_custom_command(
		OUTPUT "${RELAY_BPF_OBJECT}"
		COMMAND "${RELAY_BPF_CLANG}" -O2 -g -target bpf -D__TARGET_ARCH_x86
			"-I${RELAY_BPF_INCLUDE_DIR}" "-I${CMAKE_SOURCE_DIR}/include"
			-c "${RELAY_BPF_SOURCE}" -o "${RELAY_BPF_OBJECT}"
		DEPENDS "${RELAY_BPF_SOURCE}" "${CMAKE_SOURCE_DIR}/include/relay/relay_event.h"
		COMMENT "Compiling relay BPF program relay.bpf.c"
		VERBATIM
	)
	add_custom_target(relay_bpf_object DEPENDS "${RELAY_BPF_OBJECT}")
else()
	message(STATUS "relay: libbpf/clang not found; the relay will fall back to a TCP replica")
endif()

add_executable(relay main.cpp relay.cpp)
target_compile_features(relay PRIVATE cxx_std_20)
target_include_directories(relay PRIVATE "${CMAKE_SOURCE_DIR}/include")

if(TARGET PkgConfig::LIBBPF)
	target_link_libraries(relay PRIVATE PkgConfig::LIBBPF)
endif()

if(RELAY_BPF_OBJECT)
	add_dependencies(relay relay_bpf_object)
	target_compile_definitions(relay PRIVATE RELAY_BPF_OBJECT="${RELAY_BPF_OBJECT}")
else()
	target_compile_definitions(relay PRIVATE RELAY_BPF_OBJECT="")
endif()
