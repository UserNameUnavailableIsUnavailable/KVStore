#pragma once

namespace broker {

// Loads the sk_skb + SOCKHASH redirect program and keeps the socket maps in
// step: the upstream socket in `upstreams` (which owns the sk_skb programs) and
// the attached slave in `slaves` (the redirect target). When BPF is unavailable
// (no privileges, no object) `active()` is false and the caller falls back to a
// userspace relay.
class SockmapRedirect {
   public:
    SockmapRedirect() = default;
    ~SockmapRedirect();
    SockmapRedirect(const SockmapRedirect&) = delete;
    SockmapRedirect& operator=(const SockmapRedirect&) = delete;

    bool Open(const char* bpf_object_path);
    bool AttachUpstream(int fd);
    bool AttachSlave(int fd);
    void DetachSlave();
    bool active() const noexcept { return active_; }

   private:
    void Close();
    bool Update(int map_fd, int fd) const;

    void* object_ = nullptr;  // struct bpf_object*
    int upstream_map_fd_ = -1;
    int slave_map_fd_ = -1;
    int verdict_fd_ = -1;
    int parser_fd_ = -1;
    bool active_ = false;
};

}  // namespace broker
