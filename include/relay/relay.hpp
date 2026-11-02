#pragma once

#include <cstdint>

namespace relay {

// Loads relay.bpf.o, pins the master-port filter into the `config` map, and
// attaches the program to the egress of `ifindex` (normally `lo`, where the
// master's pushes to its slaves leave the stack). The daemon then reads the
// extracted payloads from RingFd()'s ring buffer.
//
// When loading or attaching fails -- no privileges, no object, no TC support on
// the interface -- active() is false and the daemon falls back to registering
// with the master as a /dev/null replica over TCP.
class RelayCapture {
   public:
    RelayCapture() = default;
    ~RelayCapture();
    RelayCapture(const RelayCapture&) = delete;
    RelayCapture& operator=(const RelayCapture&) = delete;

    bool Open(const char* bpf_object_path, int ifindex, std::uint16_t master_port);
    int RingFd() const noexcept { return ring_fd_; }
    bool active() const noexcept { return active_; }

   private:
    void Close();

    void* object_ = nullptr;  // struct bpf_object*
    int ring_fd_ = -1;
    int ifindex_ = 0;
    bool active_ = false;
};

}  // namespace relay