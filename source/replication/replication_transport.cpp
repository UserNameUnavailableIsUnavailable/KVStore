#include <replication/replication_daemon.hpp>
#include <replication/stream_protocol.hpp>
#include <nbio/time.hpp>
#include <algorithm>
#include <array>
#include <iostream>
#include <stdexcept>
#include <string>

#include "replay_backlog.hpp"

namespace replication {
namespace {
constexpr std::size_t kMaxClients = 128;
constexpr std::size_t kMaxFollow = 96;
constexpr auto kTimeout = std::chrono::seconds(5);
constexpr auto kHeartbeat = std::chrono::milliseconds(500);
}

struct ReplicationDaemon::Client {
    explicit Client(std::shared_ptr<nbio::net::TcpSessionService> connection)
        : session(std::move(connection)) {}
    ~Client() { Close(); }
    std::shared_ptr<nbio::net::TcpSessionService> session;
    ReplicationInfo* info = nullptr;
    std::uint64_t ticket = 0;
    std::uint64_t epoch = 0;
    std::uint64_t last = 0;
    std::uint64_t generation = 0;
    Clock::time_point progress = Clock::now();
    Clock::time_point last_frame = Clock::now();
    bool following = false;
    bool finished = false;
    bool closing = false;

    void Close() {
        if (info && ticket) {
            info->DetachReplica(ticket);
            ticket = 0;
        }
        if (!closing) session->connector().Shutdown();
        closing = true;
    }
};

ReplicationDaemon::ReplicationDaemon(ReplayBacklog& backlog, ReplicationInfo& info)
    : backlog_(backlog), info_(info) {}

ReplicationDaemon::~ReplicationDaemon() {
    Stop();
}

void ReplicationDaemon::Stop() {
    info_.daemon_ready.store(0, std::memory_order_release);
    stopping_ = true;
    for (auto& client : clients_) client->Close();
    for (auto& follower : followers_) follower.Cancel();
    changed_.NotifyAll();
}

void ReplicationDaemon::Maintain() {
    const auto now = Clock::now();
    for (auto& client : clients_) {
        if (client->finished || client->closing) continue;
        if (now - client->progress > kTimeout ||
            (client->following &&
             (client->generation != backlog_.generation ||
              !backlog_.CanFollow(client->epoch, client->last)))) {
            std::cerr << "repld: follower timed out or lost retained history\n";
            client->Close();
        }
    }
    std::erase_if(clients_, [](const auto& client) { return client->finished; });
    std::erase_if(followers_, [](const auto& follower) { return follower.is_dead(); });
    if (backlog_.head != observed_head_ || backlog_.generation != observed_generation_ ||
        now - heartbeat_ >= kHeartbeat) {
        observed_head_ = backlog_.head;
        observed_generation_ = backlog_.generation;
        heartbeat_ = now;
        changed_.NotifyAll();
    }
}

nbio::async::Task<void> ReplicationDaemon::Serve(nbio::net::Address address) {
    nbio::net::TcpListenService listener(address, kMaxClients);
    info_.daemon_ready.store(1, std::memory_order_release);
    while (!stopping_) {
        auto incoming = co_await listener.Accept();
        if (!incoming) {
            throw std::system_error(incoming.error(), "repld: accept failed");
        }
        if (clients_.size() >= kMaxClients) {
            std::cerr << "repld: follower connection limit reached\n";
            incoming->first->connector().Shutdown();
            continue;
        }
        auto client = std::make_shared<Client>(std::move(incoming->first));
        clients_.push_back(client);
        followers_.push_back(nbio::async::Spawn(Follow(client)));
    }
}

nbio::async::Task<bool> ReplicationDaemon::Send(std::shared_ptr<Client> client, std::string_view bytes) {
    while (!bytes.empty() && !client->closing) {
        auto sent = co_await client->session->Send(std::span<const char>(bytes.data(), bytes.size()));
        if (!sent || *sent == 0) {
            if (!sent) std::cerr << "repld: follower send failed: " << sent.error().message() << '\n';
            co_return false;
        }
        bytes.remove_prefix(*sent);
        client->progress = Clock::now();
    }
    co_return !client->closing;
}

nbio::async::Task<void> ReplicationDaemon::ReadPeer(std::shared_ptr<Client> client) {
    char byte;
    const auto received = co_await client->session->Receive(std::span<char>(&byte, 1));
    if (received && *received != 0) {
        std::cerr << "repld: unexpected data after FOLLOW\n";
    } else if (!received && !client->closing) {
        std::cerr << "repld: follower receive failed: " << received.error().message() << '\n';
    }
}

nbio::async::Task<void> ReplicationDaemon::Follow(std::shared_ptr<Client> client) {
    try {
        std::string request;
        std::array<char, kMaxFollow + 1> buffer;
        while (request.find('\n') == std::string::npos && !client->closing) {
            auto received = co_await client->session->Receive(
                std::span<char>(buffer.data(), buffer.size() - request.size()));
            if (!received || *received == 0) {
                throw std::runtime_error("follower disconnected during FOLLOW");
            }
            request.append(buffer.data(), *received);
            if (request.size() > kMaxFollow) throw std::runtime_error("FOLLOW request too large");
        }
        const std::string_view line(request);
        if (!line.starts_with("FOLLOW ") || !line.ends_with("\r\n") ||
            line.find('\n') != line.size() - 1) {
            throw std::runtime_error("invalid FOLLOW framing");
        }
        const auto fields = line.substr(7, line.size() - 9);
        const auto separator = fields.find(' ');
        const auto last_separator = fields.rfind(' ');
        std::uint64_t ticket = 0;
        if (separator == std::string_view::npos ||
            last_separator == separator ||
            !KV::Replication::Unsigned(fields.substr(0, separator), client->epoch) ||
            !KV::Replication::Unsigned(fields.substr(separator + 1, last_separator - separator - 1), client->last) ||
            !KV::Replication::Unsigned(fields.substr(last_separator + 1), ticket) ||
            !backlog_.CanFollow(client->epoch, client->last)) {
            throw std::runtime_error("snapshot position is not retained or stream is unhealthy");
        }
        if (!info_.ClaimReplica(ticket)) throw std::runtime_error("expired or already claimed handoff ticket");
        client->info = &info_;
        client->ticket = ticket;
        client->generation = backlog_.generation;
        client->following = true;
        const auto greeting = "+CONTINUE " + std::to_string(client->epoch) + "\r\n";
        if (co_await Send(client, greeting)) {
            co_await nbio::async::WhenAny(Write(client), ReadPeer(client));
        }
    } catch (const std::exception& error) {
        std::cerr << "repld: " << error.what() << '\n';
    }
    client->Close();
    client->finished = true;
}

nbio::async::Task<void> ReplicationDaemon::Write(std::shared_ptr<Client> client) {
    while (!stopping_ && !client->closing) {
        if (client->generation != backlog_.generation || !backlog_.CanFollow(client->epoch, client->last)) {
            std::cerr << "repld: follower lost stream continuity\n";
            co_return;
        }
        const auto* record = backlog_.Next(client->last);
        const auto now = Clock::now();
        if (record || now - client->last_frame >= kHeartbeat) {
            const auto offset = record ? record->end_offset : client->last;
            // Capture may reclaim history while a socket send is suspended.
            const std::string payload = record ? record->payload : std::string{};
            const auto header = "@" + std::to_string(client->epoch) + " " + std::to_string(offset) +
                                " " + std::to_string(payload.size()) + "\r\n";
            if (!co_await Send(client, header) || !co_await Send(client, payload)) co_return;
            client->last = offset;
            client->last_frame = Clock::now();
        } else {
            co_await changed_.Wait([this, &client] {
                return stopping_ || client->closing || client->generation != backlog_.generation ||
                       backlog_.head != client->last || Clock::now() - client->last_frame >= kHeartbeat;
            });
        }
    }
}
}  // namespace replication
