# The broker links libbpf so it can install the sk_skb + sockhash redirect whose
# programs live in bpf/redirect.bpf.c. Both halves are optional at run time: if
# the object is missing or privileges are missing, the broker relays in
# userspace instead, so a failed BPF build must not fail the whole project.

find_package(PkgConfig QUIET)
set(BROKER_BPF_OBJECT "")

if(PkgConfig_FOUND)
	pkg_check_modules(LIBBPF IMPORTED_TARGET libbpf)
endif()

if(LIBBPF_FOUND)
	find_program(BROKER_BPF_CLANG NAMES clang)
	# bpf/bpf_helpers.h is shipped with libbpf-dev alongside libbpf.h.
	find_path(BROKER_BPF_INCLUDE_DIR bpf/bpf_helpers.h)
endif()

if(LIBBPF_FOUND AND BROKER_BPF_CLANG AND BROKER_BPF_INCLUDE_DIR)
	# clang must target bpf for the object; the C++ side compiles with the normal
	# compiler, so this is a separate custom command rather than a normal target.
	set(BROKER_BPF_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/bpf/redirect.bpf.c")
	set(BROKER_BPF_OBJECT "${CMAKE_CURRENT_BINARY_DIR}/redirect.bpf.o")
	add_custom_command(
		OUTPUT "${BROKER_BPF_OBJECT}"
		COMMAND "${BROKER_BPF_CLANG}" -O2 -g -target bpf -D__TARGET_ARCH_x86
			-I"${BROKER_BPF_INCLUDE_DIR}" -c "${BROKER_BPF_SOURCE}" -o "${BROKER_BPF_OBJECT}"
		DEPENDS "${BROKER_BPF_SOURCE}"
		COMMENT "Compiling broker BPF program redirect.bpf.c"
		VERBATIM
	)
	add_custom_target(broker_bpf_object DEPENDS "${BROKER_BPF_OBJECT}")
else()
	message(STATUS "broker: libbpf/clang not found; the broker will relay in userspace only")
endif()

add_executable(broker main.cpp redirect.cpp)
target_compile_features(broker PRIVATE cxx_std_20)

if(TARGET PkgConfig::LIBBPF)
	target_link_libraries(broker PRIVATE PkgConfig::LIBBPF)
endif()

if(BROKER_BPF_OBJECT)
	add_dependencies(broker broker_bpf_object)
	target_compile_definitions(broker PRIVATE BROKER_BPF_OBJECT="${BROKER_BPF_OBJECT}")
else()
	target_compile_definitions(broker PRIVATE BROKER_BPF_OBJECT="")
endif()
