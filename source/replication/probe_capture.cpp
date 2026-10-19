#include "probe_capture.hpp"
#include "replay_backlog.hpp"
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <cerrno>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <system_error>
#include <time.h>

namespace replication {
struct ProbeCapture::State {
    ~State() {
        ring_buffer__free(ring);
        bpf_link__destroy(link);
        bpf_object__close(object);
    }
    static int Event(void* context, void* data, std::size_t size) noexcept {
        auto& self = *static_cast<State*>(context);
        if (!self.active || !self.error.empty()) return 0;
        try {
            if (size != sizeof(kv_log_event)) throw std::runtime_error("invalid probe event size");
            kv_log_event event;
            std::memcpy(&event, data, sizeof(event));
            self.backlog->Consume(self.stream.Consume(event));
        } catch (const std::exception& error) {
            self.error = error.what();
        }
        return 0;
    }
    bpf_object* object = nullptr;
    bpf_link* link = nullptr;
    ring_buffer* ring = nullptr;
    int status_fd = -1;
    ReplayBacklog* backlog = nullptr;
    CaptureStream stream;
    bool active = false;
    std::string error;
    Clock::time_point attached = Clock::now();
    Clock::time_point stable = Clock::now();
};

ProbeCapture::ProbeCapture(int pid, const std::string& binary, const std::string& object)
    : state_(std::make_unique<State>()) {
    auto& state = *state_;
    state.object = bpf_object__open_file(object.c_str(), nullptr);
    if (!state.object) throw std::system_error(errno, std::generic_category(), "opening BPF object");
    if (const int error = bpf_object__load(state.object); error) {
        throw std::system_error(-error, std::generic_category(), "loading write-log uprobe");
    }
    auto* program = bpf_object__find_program_by_name(state.object, "write_log");
    if (!program) throw std::runtime_error("BPF object has no write_log program");
    bpf_uprobe_opts options{};
    options.sz = sizeof(options);
    options.func_name = "kvstore_write_log_v1";
    state.link = bpf_program__attach_uprobe_opts(program, pid, binary.c_str(), 0, &options);
    if (!state.link) throw std::system_error(errno, std::generic_category(), "attaching write-log uprobe");
    state.status_fd = bpf_object__find_map_fd_by_name(state.object, "status");
    const int ring_fd = bpf_object__find_map_fd_by_name(state.object, "records");
    if (state.status_fd < 0 || ring_fd < 0) throw std::runtime_error("write-log BPF maps missing");
    state.ring = ring_buffer__new(ring_fd, State::Event, &state, nullptr);
    if (!state.ring) throw std::system_error(errno, std::generic_category(), "opening BPF ring buffer");
}

ProbeCapture::~ProbeCapture() = default;

void ProbeCapture::Poll(ReplayBacklog& backlog) {
    auto& state = *state_;
    state.backlog = &backlog;
    const std::uint32_t key = 0;
    kv_log_state before{}, after{};
    if (bpf_map_lookup_elem(state.status_fd, &key, &before) ||
        bpf_map_lookup_elem(state.status_fd, &key, &after)) {
        throw std::system_error(errno, std::generic_category(), "reading probe status");
    }
    if ((before.revision & 1) || before.revision != after.revision) {
        if (Clock::now() - state.stable > std::chrono::seconds(5)) {
            throw std::runtime_error("write-log probe metadata remained unstable for five seconds");
        }
        return;
    }
    state.stable = Clock::now();
    if (!before.epoch) {
        if (Clock::now() - state.attached > std::chrono::seconds(5)) {
            throw std::runtime_error("write-log hook was not observed within five seconds");
        }
        return;
    }
    timespec now{};
    if (::clock_gettime(CLOCK_MONOTONIC, &now)) {
        throw std::system_error(errno, std::generic_category(), "reading monotonic time");
    }
    const auto ns = static_cast<std::uint64_t>(now.tv_sec) * 1000000000ULL + now.tv_nsec;
    if (ns > before.seen_ns && ns - before.seen_ns > 5000000000ULL) {
        throw std::runtime_error("master write-log probe heartbeat timed out");
    }
    auto reset = state.stream.Observe(before);
    if (!reset && !state.error.empty()) reset = state.stream.Reset(before.epoch, before.head);
    if (reset) {
        if (state.active) std::cerr << "repld: capture loss; rebasing at " << before.head
                                   << " (" << state.error << ")\n";
        backlog.Consume(std::move(*reset));
        state.error.clear();
        state.active = true;
    }
    const int result = ring_buffer__consume(state.ring);
    if (result < 0) throw std::system_error(-result, std::generic_category(), "consuming probe records");
    // Never leave existing followers healthy after a malformed/truncated event.
    if (!state.error.empty()) backlog.Invalidate();
}
}  // namespace replication
