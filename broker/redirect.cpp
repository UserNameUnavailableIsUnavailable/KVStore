#include "redirect.hpp"

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <linux/bpf.h>

#include <cerrno>
#include <cstdio>
#include <cstring>

namespace broker {
namespace {
// One upstream and one slave is all a broker needs: a second slave replaces the
// first, and the upstream is re-inserted after every reconnect.
constexpr int kSlot = 0;
}  // namespace

SockmapRedirect::~SockmapRedirect() { Close(); }

bool SockmapRedirect::Open(const char* bpf_object_path) {
    if (bpf_object_path == nullptr || *bpf_object_path == '\0') {
        return false;
    }
    auto* object = bpf_object__open_file(bpf_object_path, nullptr);
    if (object == nullptr) {
        std::fprintf(stderr, "broker: cannot open BPF object %s\n", bpf_object_path);
        return false;
    }
    object_ = object;
    if (const int error = bpf_object__load(object); error != 0) {
        // libbpf has already printed the verifier log; repeat the cause in our own
        // words so the broker's output is self-contained.
        std::fprintf(stderr, "broker: cannot load BPF object (%d: %s); using userspace relay\n", error,
                     std::strerror(-error));
        Close();
        return false;
    }
    // upstreams is a SOCKHASH (arbitrary socket registration, and it owns the
    // sk_skb programs); slaves is a SOCKMAP, because only a SOCKMAP may be named
    // by bpf_sk_redirect_map -- a SOCKHASH pairs with bpf_sk_redirect_hash.
    bpf_map* upstreams = bpf_object__find_map_by_name(object, "upstreams");
    bpf_map* slaves = bpf_object__find_map_by_name(object, "slaves");
    bpf_program* verdict = bpf_object__find_program_by_name(object, "broker_verdict");
    bpf_program* parser = bpf_object__find_program_by_name(object, "broker_parser");
    if (upstreams == nullptr || slaves == nullptr || verdict == nullptr || parser == nullptr) {
        std::fprintf(stderr, "broker: BPF object is missing its maps or programs\n");
        Close();
        return false;
    }
    upstream_map_fd_ = bpf_map__fd(upstreams);
    slave_map_fd_ = bpf_map__fd(slaves);
    verdict_fd_ = bpf_program__fd(verdict);
    parser_fd_ = bpf_program__fd(parser);
    // Only the upstream map gets the programs: a slave socket must never be
    // verdict-processed, or its own ingress could be redirected back into it.
    if (bpf_prog_attach(verdict_fd_, upstream_map_fd_, BPF_SK_SKB_STREAM_VERDICT, 0) != 0 ||
        bpf_prog_attach(parser_fd_, upstream_map_fd_, BPF_SK_SKB_STREAM_PARSER, 0) != 0) {
        std::fprintf(stderr, "broker: cannot attach sk_skb programs; using userspace relay\n");
        Close();
        return false;
    }
    active_ = true;
    return true;
}

bool SockmapRedirect::Update(int map_fd, int fd) const {
    const __u32 key = static_cast<__u32>(kSlot);
    return bpf_map_update_elem(map_fd, &key, &fd, BPF_ANY) == 0;
}

bool SockmapRedirect::AttachUpstream(int fd) { return active_ && Update(upstream_map_fd_, fd); }

bool SockmapRedirect::AttachSlave(int fd) { return active_ && Update(slave_map_fd_, fd); }

void SockmapRedirect::DetachSlave() {
    if (!active_) {
        return;
    }
    const __u32 key = static_cast<__u32>(kSlot);
    bpf_map_delete_elem(slave_map_fd_, &key);
}

void SockmapRedirect::Close() {
    if (verdict_fd_ >= 0 && upstream_map_fd_ >= 0) {
        bpf_prog_detach2(verdict_fd_, upstream_map_fd_, BPF_SK_SKB_STREAM_VERDICT);
    }
    if (parser_fd_ >= 0 && upstream_map_fd_ >= 0) {
        bpf_prog_detach2(parser_fd_, upstream_map_fd_, BPF_SK_SKB_STREAM_PARSER);
    }
    if (object_ != nullptr) {
        bpf_object__close(static_cast<bpf_object*>(object_));
    }
    object_ = nullptr;
    upstream_map_fd_ = slave_map_fd_ = verdict_fd_ = parser_fd_ = -1;
    active_ = false;
}
}  // namespace broker
