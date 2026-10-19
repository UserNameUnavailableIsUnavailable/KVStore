#pragma once

#include <replication/replication_info.hpp>
#include <string>

namespace replication {
class RepldProcess {
public:
    RepldProcess(SharedReplicationInfo& info, const std::string& executable,
                 const std::string& address, unsigned port, const std::string& backend);
    ~RepldProcess();
    RepldProcess(const RepldProcess&) = delete;
    RepldProcess& operator=(const RepldProcess&) = delete;
    void Check();
private:
    void Stop() noexcept;
    int pid_ = -1;
};
}  // namespace replication
