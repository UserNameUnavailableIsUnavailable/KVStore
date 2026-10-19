#pragma once

#include <memory>
#include <string>

namespace replication {
class ReplayBacklog;

class ProbeCapture {
public:
    ProbeCapture(int pid, const std::string& binary, const std::string& object);
    ~ProbeCapture();
    ProbeCapture(const ProbeCapture&) = delete;
    ProbeCapture& operator=(const ProbeCapture&) = delete;
    void Poll(ReplayBacklog& backlog);
private:
    struct State;
    std::unique_ptr<State> state_;
};
}  // namespace replication
