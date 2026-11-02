#include <relay/relay.hpp>

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <linux/bpf.h>

#include <relay/relay_event.h>

#include <cerrno>
#include <cstdio>

namespace relay {

RelayCapture::~RelayCapture() { Close(); }

bool RelayCapture::Open(const char* bpf_object_path, int ifindex, std::uint16_t master_port) {
    if (bpf_object_path == nullptr || *bpf_object_path == '\0' || ifindex <= 0) {
        return false;
    }
    auto* object = bpf_object__open_file(bpf_object_path, nullptr);
    if (object == nullptr) {
        std::fprintf(stderr, "relay: cannot open BPF object %s\n", bpf_object_path);
        return false;
    }
    object_ = object;
    if (const int error = bpf_object__load(object); error != 0) {
        std::fprintf(stderr, "relay: cannot load BPF object (%d); falling back to a TCP replica\n", error);
        Close();
        return false;
    }
    bpf_map* events = bpf_object__find_map_by_name(object, "events");
    bpf_map* config = bpf_object__find_map_by_name(object, "config");
    bpf_program* program = bpf_object__find_program_by_name(object, "relay_egress");
    if (events == nullptr || config == nullptr || program == nullptr) {
        std::fprintf(stderr, "relay: BPF object is missing its map or program\n");
        Close();
        return false;
    }

    // Pin the master-port filter before attaching, so the program never taps
    // anything by accident with a default port.
    const __u32 key = 0;
    struct relay_config value {};
    value.master_port = master_port;
    if (bpf_map_update_elem(bpf_map__fd(config), &key, &value, BPF_ANY) != 0) {
        std::fprintf(stderr, "relay: cannot set the master port\n");
        Close();
        return false;
    }

    LIBBPF_OPTS(bpf_tc_hook, hook, .ifindex = ifindex, .attach_point = BPF_TC_EGRESS);
    int error = bpf_tc_hook_create(&hook);
    if (error != 0 && error != -EEXIST) {
        std::fprintf(stderr, "relay: cannot create the TC egress hook (%d); falling back to a TCP replica\n", error);
        Close();
        return false;
    }
    LIBBPF_OPTS(bpf_tc_opts, opts, .prog_fd = bpf_program__fd(program), .flags = BPF_TC_F_REPLACE);
    error = bpf_tc_attach(&hook, &opts);
    if (error != 0) {
        std::fprintf(stderr, "relay: cannot attach to TC egress (%d); falling back to a TCP replica\n", error);
        Close();
        return false;
    }

    ring_fd_ = bpf_map__fd(events);
    ifindex_ = ifindex;
    active_ = true;
    return true;
}

void RelayCapture::Close() {
    if (active_) {
        LIBBPF_OPTS(bpf_tc_hook, hook, .ifindex = ifindex_, .attach_point = BPF_TC_EGRESS);
        LIBBPF_OPTS(bpf_tc_opts, opts);
        bpf_tc_detach(&hook, &opts);
        bpf_tc_hook_destroy(&hook);
    }
    if (object_ != nullptr) {
        bpf_object__close(static_cast<bpf_object*>(object_));
    }
    object_ = nullptr;
    ring_fd_ = -1;
    ifindex_ = 0;
    active_ = false;
}

}  // namespace relay
