// The RDMA link's throughput, measured the way the replication link uses it: one
// chunk per message, as many messages in flight as the receiver can take, and a
// receiver that reports what it has taken so the sender stays inside what it has
// posted.
//
// Both ends are in this process, on two engines in two threads -- which is what
// two servers are, one engine each -- because what is being measured is the
// transfer rather than the deployment, and a benchmark whose two halves have to be
// started by hand in two terminals is one nobody runs.
//
// The message size belongs to the resource manager rather than to this program:
// the chunks it registers are what a send is handed and what a receive lands in,
// so the size printed here is the manager's and it moves with the manager when the
// manager's own layout becomes a parameter. There is deliberately no `--chunk`: a
// message larger than a receive chunk is truncated by the device rather than
// refused, so the two sizes have to agree by construction and not by argument.
//
// The flow control is the part that cannot be left out. A receive is a buffer the
// receiver posted, and a message that arrives with none posted is not queued and
// not refused: the sender is told to retry, and a queue pair that runs out of
// retries is torn down with RNR_RETRY_EXC_ERR. A sender gated only by its own send
// completions does not see that coming, because a completion says the message was
// *delivered*, not that the receiver has finished with the buffer it landed in --
// so it will eventually be a whole window ahead of the receiver and break the
// connection. Hence the credits below, which are the same window the replication
// transfer runs on, and hence the count printed with every run.

#if defined(__linux__)

#include <Foundation/Core/BitmapMemory.hpp>
#include <Foundation/Core/Byte.hpp>
#include <Foundation/Core/Expected.hpp>
#include <Foundation/Core/RdmaAcceptor.hpp>
#include <Foundation/Core/RdmaConnector.hpp>
#include <Foundation/Core/RdmaResourceManager.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/EpollMultiplexer.hpp>
#include <Foundation/NBIO/NBIO.hpp>
#include <Foundation/NBIO/RdmaAcceptChannel.hpp>
#include <Foundation/NBIO/RdmaConnectChannel.hpp>
#include <Foundation/NBIO/RdmaSessionService.hpp>
#include <Foundation/NBIO/URingMultiplexer.hpp>

#include <CLI/CLI.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace
{
namespace Core = Foundation::Core;
namespace NBIO = Foundation::NBIO;

using Clock = std::chrono::steady_clock;

// A gibibyte, which is the payload a snapshot tends to be.
constexpr std::size_t kDefaultPayload = 1U << 30;

// The most the sender may have uncredited at once. A receiver posts one receive
// per receive chunk and the device needs one for every message in flight, so this
// is not a tuning knob: it is the receiver's own depth, and the number the window
// has to stay inside to keep the connection alive.
constexpr std::size_t kWindow = Core::RdmaConnector::kReceiveChunks;

// What one end of a transfer ended up with. `ok` is false for a transfer that did
// not finish, and `failure` then says why -- the one thing either end reports.
struct Outcome
{
    bool ok{false};
    std::size_t bytes{0};
    double seconds{0.0};
    std::string failure;
};

// How far each end has got, for the watcher: the first question about a transfer
// that never finishes is which end stopped.
struct Progress
{
    std::atomic<std::size_t> sent{0};
    std::atomic<std::size_t> taken{0};
};

// The control plane: every message that is not the payload says what it is and
// carries numbers. A kind per message is what lets the sender tell a credit from
// the receiver's answer without counting messages to know which one is which.
enum Kind : std::uint64_t
{
    kHeader = 1, // the payload size and the message size
    kCredit = 2, // messages the receiver has taken and finished with
    kTaken = 3,  // the answer: the bytes the receiver ended up with
};

// One shape for all of them, so the decode is a read of three numbers rather than
// a parse: [kind][value][extra], unused fields zero.
constexpr std::size_t kReportBytes = 3 * sizeof(std::uint64_t);

struct Report
{
    std::uint64_t kind{0};
    std::uint64_t value{0};
    std::uint64_t extra{0};
};

// The numbers on the wire are written and read in network byte order, in the
// width the buffer already says: a report is three quad words, and a quad word
// that is read on a little-endian machine is read the other way round.
void PutU64(char *out, std::uint64_t value) noexcept
{
    const auto ordered = Core::to_big_endian(value);
    std::memcpy(out, &ordered, sizeof(ordered));
}

std::uint64_t GetU64(const char *in) noexcept
{
    std::uint64_t ordered = 0;
    std::memcpy(&ordered, in, sizeof(ordered));
    return Core::from_big_endian(ordered);
}

double Seconds(Clock::time_point from, Clock::time_point to) noexcept
{
    return std::chrono::duration<double>(to - from).count();
}

std::optional<Report> Decode(std::span<const char> message) noexcept
{
    if (message.size() < kReportBytes)
    {
        return std::nullopt;
    }
    return Report{.kind = GetU64(message.data()),
                  .value = GetU64(message.data() + sizeof(std::uint64_t)),
                  .extra = GetU64(message.data() + 2 * sizeof(std::uint64_t))};
}

std::array<char, kReportBytes> Encode(const Report &report) noexcept
{
    std::array<char, kReportBytes> message{};
    PutU64(message.data(), report.kind);
    PutU64(message.data() + sizeof(std::uint64_t), report.value);
    PutU64(message.data() + 2 * sizeof(std::uint64_t), report.extra);
    return message;
}

// Waits for one message, skipping the wake-ups that carry nothing. A receive
// answers empty whenever the multiplexer reported a readiness that turns out not
// to be a completion for this direction: a wake-up, not an end to anything -- the
// end of a link arrives as an error, because a link that has gone can never
// satisfy the wait.
NBIO::Task<Core::expected<std::span<char>, std::string>> NextMessage(NBIO::RdmaSessionService &session)
{
    while (true)
    {
        auto incoming = co_await session.receive();
        if (!incoming) [[unlikely]]
        {
            co_return Core::unexpected(incoming.error());
        }
        if (*incoming)
        {
            co_return **incoming;
        }
    }
}

// One message off the send pool, waited for: the control plane carries a handful
// of messages per transfer, and each one is what the other end is waiting on.
NBIO::Task<Core::expected<void, std::string>> SendReport(NBIO::RdmaSessionService &session, const Report &report)
{
    const auto message = Encode(report);
    while (true)
    {
        auto acquired = session.send_channel().acquire();
        if (!acquired) [[unlikely]]
        {
            co_return Core::unexpected(acquired.error());
        }
        if (!*acquired) [[unlikely]]
        {
            // Every chunk is in flight: one has to come back before this can go
            // out. Waiting for exactly one is what keeps this from waiting for the
            // payload as well.
            const auto reaped = co_await session.poll_send(1);
            if (!reaped) [[unlikely]]
            {
                co_return Core::unexpected(reaped.error());
            }
            if (*reaped == 0 && session.send_channel().outstanding() != 0) [[unlikely]]
            {
                co_return Core::unexpected(std::string{"the link stopped reporting completions"});
            }
            continue;
        }

        const std::span<char> chunk = **acquired;
        if (chunk.size() < message.size()) [[unlikely]]
        {
            co_return Core::unexpected(std::string{"a chunk cannot carry a report"});
        }
        std::memcpy(chunk.data(), message.data(), message.size());
        const auto posted = session.send(chunk, message.size());
        if (!posted) [[unlikely]]
        {
            co_return Core::unexpected(posted.error());
        }
        co_return Core::expected<void, std::string>{};
    }
}

// Takes in every credit that has already arrived and answers with the count that
// stands. Reading them all in one pass is what keeps a window that has opened up
// from being used one message per wake-up, which would make the transfer a round
// trip per message rather than a stream.
NBIO::Task<Core::expected<void, std::string>> HarvestCredits(NBIO::RdmaSessionService &session, std::size_t &credited)
{
    while (true)
    {
        auto incoming = co_await session.try_receive();
        if (!incoming) [[unlikely]]
        {
            co_return Core::unexpected(incoming.error());
        }
        if (!*incoming)
        {
            co_return Core::expected<void, std::string>{};
        }

        const std::span<char> message = **incoming;
        const auto size = message.size();
        const auto report = Decode(message);
        // Handed back before the report is acted on, so a chunk is never held
        // while a failure is being built from what it held.
        (void)session.release(message);

        if (!report || report->kind != kCredit) [[unlikely]]
        {
            co_return Core::unexpected("the receiver sent a report of " + std::to_string(size) +
                                       " bytes that is not a credit");
        }
        if (report->value < credited) [[unlikely]]
        {
            // Credits count messages finished, so one that goes backwards is a
            // report about a transfer other than this one -- and acting on it
            // would open the window on a count that never happened.
            co_return Core::unexpected("the receiver credited " + std::to_string(report->value) +
                                       " messages after crediting " + std::to_string(credited));
        }
        credited = static_cast<std::size_t>(report->value);
    }
}

// Waits for one credit, with the window full and the next message held by it.
// Everything already waiting was taken in by the caller, so this is one report and
// no more -- and it is the wait that makes this a measurement of the receiver as
// much as of the device.
NBIO::Task<Core::expected<void, std::string>> WaitForCredit(NBIO::RdmaSessionService &session, std::size_t &credited)
{
    auto incoming = co_await NextMessage(session);
    if (!incoming) [[unlikely]]
    {
        co_return Core::unexpected(incoming.error());
    }
    const auto report = Decode(*incoming);
    (void)session.release(*incoming);
    if (!report || report->kind != kCredit) [[unlikely]]
    {
        co_return Core::unexpected(std::string{"the receiver stopped reporting credits"});
    }
    if (report->value <= credited) [[unlikely]]
    {
        co_return Core::unexpected("the receiver credited " + std::to_string(report->value) +
                                   " messages after crediting " + std::to_string(credited));
    }
    credited = static_cast<std::size_t>(report->value);
    co_return Core::expected<void, std::string>{};
}

// The sending half: announce the payload, put it on the wire as fast as the window
// and the stream's own chunks allow, then wait for the receiver to say what it
// ended up with. A posted send is not a sent one, so the clock stops when the last
// completion comes back rather than when the last message is handed over --
// otherwise this would measure how fast the loop can fill chunks.
NBIO::Task<void> Send(NBIO::RdmaAcceptChannel &channel, const std::vector<char> &payload, std::size_t message,
                      Outcome &outcome, Progress &progress)
{
    auto admitted = co_await channel.accept();
    if (!admitted) [[unlikely]]
    {
        outcome.failure = "the connection was not admitted: " + admitted.error();
        co_return;
    }
    NBIO::RdmaSessionService &session = **admitted;

    const auto messages = (payload.size() + message - 1) / message;

    // The receiver is told what is coming before anything is: what it is counting
    // towards, and the size the messages are cut at, which is also what says a
    // message it took whole is a message it took whole.
    const Report header{.kind = kHeader, .value = payload.size(), .extra = message};
    if (const auto announced = co_await SendReport(session, header); !announced) [[unlikely]]
    {
        outcome.failure = "the payload could not be announced: " + announced.error();
        co_return;
    }

    const auto started = Clock::now();
    std::size_t sent = 0;
    std::size_t credited = 0;
    while (sent < messages)
    {
        if (const auto harvested = co_await HarvestCredits(session, credited); !harvested) [[unlikely]]
        {
            outcome.failure = harvested.error();
            co_return;
        }
        if (sent - credited >= kWindow)
        {
            // The window is full: the receiver has as many messages to account for
            // as it has receives posted, and nothing more may go out until it says
            // it has finished with one.
            if (const auto credited_now = co_await WaitForCredit(session, credited); !credited_now) [[unlikely]]
            {
                outcome.failure = "the receiver stopped reporting after " + std::to_string(sent) + " messages: " +
                                  credited_now.error();
                co_return;
            }
            continue;
        }

        auto acquired = session.send_channel().acquire();
        if (!acquired) [[unlikely]]
        {
            outcome.failure = "the send channel failed: " + acquired.error();
            co_return;
        }
        if (!*acquired)
        {
            // Every chunk is in flight, so one has to be retired before another
            // message can be built. This is the sender's own limit -- the tighter
            // of the two when the window is wider -- and it is what keeps the
            // device as busy as it will go.
            const auto reaped = co_await session.poll_send(1);
            if (!reaped) [[unlikely]]
            {
                outcome.failure = "the link stopped reporting completions after " + std::to_string(sent) +
                                  " messages: " + reaped.error();
                co_return;
            }
            if (*reaped == 0 && session.send_channel().outstanding() != 0) [[unlikely]]
            {
                outcome.failure = "the link stopped reporting completions after " + std::to_string(sent) + " messages";
                co_return;
            }
            continue;
        }

        const std::span<char> chunk = **acquired;
        const auto take = std::min(chunk.size(), payload.size() - sent * message);
        std::memcpy(chunk.data(), payload.data() + sent * message, take);
        const auto posted = session.send(chunk, take);
        if (!posted) [[unlikely]]
        {
            outcome.failure = "sending message " + std::to_string(sent) + " failed: " + posted.error();
            co_return;
        }
        ++sent;
        progress.sent.store(sent, std::memory_order_relaxed);
    }

    // Everything this end posted has to come back before the clock stops: the bytes
    // of the last messages are still on their way out of the device.
    while (session.send_channel().outstanding() != 0)
    {
        const auto reaped = co_await session.poll_send();
        if (!reaped) [[unlikely]]
        {
            outcome.failure = "the link stopped reporting completions after " + std::to_string(sent) +
                              " messages: " + reaped.error();
            co_return;
        }
        if (*reaped == 0 && session.send_channel().outstanding() != 0) [[unlikely]]
        {
            outcome.failure = "the link stopped reporting completions after " + std::to_string(sent) + " messages";
            co_return;
        }
    }
    const auto finished = Clock::now();

    // The receiver's own count, which is the only thing that says the payload
    // arrived rather than merely left. The link is kept up until it does, so neither
    // end is tearing its queue pair down while the other is still taking messages
    // off it. Credits that arrived after the last message was posted are skipped
    // rather than mistaken for the answer: the sender stops posting the moment it
    // has posted them all, so it can be behind on reading its own reports.
    while (true)
    {
        auto answer = co_await NextMessage(session);
        if (!answer) [[unlikely]]
        {
            outcome.failure = "the receiver never answered: " + answer.error();
            co_return;
        }
        const auto report = Decode(*answer);
        (void)session.release(*answer);
        if (!report) [[unlikely]]
        {
            outcome.failure = "the receiver answered in " + std::to_string(answer->size()) + " bytes";
            co_return;
        }
        if (report->kind == kCredit)
        {
            continue;
        }
        if (report->kind != kTaken) [[unlikely]]
        {
            outcome.failure = "the receiver answered with something other than a count";
            co_return;
        }
        if (report->value != payload.size()) [[unlikely]]
        {
            outcome.failure = "the receiver took " + std::to_string(report->value) + " of " +
                              std::to_string(payload.size()) + " bytes";
            co_return;
        }
        break;
    }

    outcome.bytes = sent * message;
    outcome.seconds = Seconds(started, finished);
    outcome.ok = true;
}

// The receiving half: take the announced count, then every message that follows
// until that many bytes have arrived, crediting each one back except the last --
// the answer says what a credit for that one would have said, and a credit of its
// own would be the same message twice. The clock runs from just after the
// announcement to the last byte, which is the window in which the payload was on
// the wire.
NBIO::Task<void> Receive(NBIO::RdmaConnectChannel &channel, Core::SocketAddress master, std::size_t chunk,
                         Outcome &outcome, Progress &progress)
{
    auto connected = co_await channel.connect(master);
    if (!connected) [[unlikely]]
    {
        outcome.failure = "the connection was not established: " + connected.error();
        co_return;
    }
    NBIO::RdmaSessionService &session = **connected;

    auto announcement = co_await NextMessage(session);
    if (!announcement) [[unlikely]]
    {
        outcome.failure = "the sender never said what was coming: " + announcement.error();
        co_return;
    }
    const auto report = Decode(*announcement);
    (void)session.release(*announcement);
    if (!report || report->kind != kHeader) [[unlikely]]
    {
        outcome.failure = "the sender did not announce a payload";
        co_return;
    }
    const std::size_t total = report->value;
    const std::size_t message = report->extra;
    if (message == 0 || message > chunk) [[unlikely]]
    {
        // More than a receive chunk holds is truncated by the device rather than
        // refused, and the count would come up short of what was sent with nothing
        // to say so.
        outcome.failure = "the sender cut the payload at " + std::to_string(message) + " bytes and a receive holds " +
                          std::to_string(chunk);
        co_return;
    }
    const auto messages = (total + message - 1) / message;
    const auto last = total - (messages - 1) * message;

    const auto started = Clock::now();
    std::size_t taken = 0;
    std::size_t bytes = 0;
    while (bytes < total)
    {
        auto incoming = co_await NextMessage(session);
        if (!incoming) [[unlikely]]
        {
            outcome.failure = "the link ended after " + std::to_string(bytes) + " of " + std::to_string(total) +
                              " bytes: " + incoming.error();
            co_return;
        }
        const std::span<char> message_bytes = *incoming;
        const auto length = message_bytes.size();
        // Checked before the chunk goes back: a message that is not the size the
        // sender announced is a truncated one, and counting it would report a
        // transfer that never happened.
        const auto wanted = taken + 1 == messages ? last : message;
        if (length != wanted) [[unlikely]]
        {
            outcome.failure = "message " + std::to_string(taken) + " is " + std::to_string(length) + " bytes where " +
                              std::to_string(wanted) + " were announced";
            (void)session.release(message_bytes);
            co_return;
        }
        // Handed back before the credit goes out, so a credit is always the proof
        // that this end has the buffer back to receive the next one into.
        if (const auto released = session.release(message_bytes); !released) [[unlikely]]
        {
            outcome.failure = "handing a message back failed: " + released.error();
            co_return;
        }
        bytes += length;
        ++taken;
        progress.taken.store(taken, std::memory_order_relaxed);

        if (taken < messages)
        {
            const Report credit{.kind = kCredit, .value = taken};
            if (const auto posted = co_await SendReport(session, credit); !posted) [[unlikely]]
            {
                outcome.failure = "the credit for message " + std::to_string(taken) + " could not be sent: " +
                                  posted.error();
                co_return;
            }
        }
    }
    const auto finished = Clock::now();

    // The answer goes out before anything else: the sender is holding its queue
    // pair open for it, and this end is the one that knows the payload landed.
    const Report answer{.kind = kTaken, .value = bytes};
    if (const auto posted = co_await SendReport(session, answer); !posted) [[unlikely]]
    {
        outcome.failure = "the answer could not be sent: " + posted.error();
        co_return;
    }
    // A posted send is not a sent one, and this session is dropped the moment this
    // returns, so let the answer leave the device first.
    if (const auto settled = co_await session.poll_send(0); !settled) [[unlikely]]
    {
        outcome.failure = "the answer was never reported as sent: " + settled.error();
        co_return;
    }

    outcome.bytes = bytes;
    outcome.seconds = Seconds(started, finished);
    outcome.ok = true;
}

// A port to listen on that nothing else has. An RDMA listener cannot ask the
// kernel for one the way a TCP listener can -- rdma_bind_addr with port 0 leaves
// the id on a port nobody can name -- so the free port is asked of the TCP stack
// and then used for the RDMA listener, as the RDMA tests do.
std::uint16_t FreePort()
{
    const int probe = ::socket(AF_INET, SOCK_STREAM, 0);
    if (probe < 0)
    {
        return 0;
    }

    ::sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    std::uint16_t port = 0;
    if (::bind(probe, reinterpret_cast<::sockaddr *>(&address), sizeof(address)) == 0)
    {
        ::socklen_t length = sizeof(address);
        if (::getsockname(probe, reinterpret_cast<::sockaddr *>(&address), &length) == 0)
        {
            port = ::ntohs(address.sin_port);
        }
    }
    ::close(probe);
    return port;
}

std::unique_ptr<NBIO::Multiplexer> MakeMultiplexer(const std::string &name)
{
    if (name == "epoll")
    {
        return std::make_unique<NBIO::EpollMultiplexer>();
    }
    if (name == "io_uring")
    {
        return std::make_unique<NBIO::URingMultiplexer>();
    }
    throw std::invalid_argument("--multiplexer must be 'epoll' or 'io_uring'");
}

// Installs an engine on this thread and drives one end of the transfer on it. The
// only thing that can throw here is what is built before the transfer starts -- an
// engine, an id, a channel -- and a run has one place its outcome is written, so it
// is written from here too.
template <typename Body> void RunEngine(Body body, const std::string &multiplexer, Outcome &outcome)
{
    try
    {
        NBIO::initialize(MakeMultiplexer(multiplexer));
        body();
    }
    catch (const std::exception &error)
    {
        outcome.failure = error.what();
    }
    catch (...)
    {
        outcome.failure = "unknown failure";
    }
}

// splitmix64: one multiply per word, and random enough that no part of the path can
// treat the payload as a pattern it recognises and skip.
void FillRandom(std::vector<char> &payload)
{
    constexpr std::uint64_t kGolden = 0x9E3779B97F4A7C15ULL;
    std::uint64_t state = kGolden;
    for (std::size_t offset = 0; offset + sizeof(std::uint64_t) <= payload.size(); offset += sizeof(std::uint64_t))
    {
        state += kGolden;
        std::uint64_t mixed = state;
        mixed = (mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9ULL;
        mixed = (mixed ^ (mixed >> 27)) * 0x94D049BB133111EBULL;
        mixed ^= mixed >> 31;
        std::memcpy(payload.data() + offset, &mixed, sizeof(mixed));
    }
}

// The bytes to move. Generated rather than read, because a file would put the page
// cache and the filesystem in front of the wire and this is a measurement of the
// wire -- but a file can be named, for the run whose payload has to be the snapshot
// a server would send. Either way the whole buffer is written here, so no page
// fault of its own lands in the transfer.
bool LoadPayload(const std::string &file, std::size_t size, std::vector<char> &payload)
{
    if (file.empty())
    {
        payload.resize(size);
        FillRandom(payload);
        return true;
    }

    std::ifstream stream(file, std::ios::binary | std::ios::ate);
    if (!stream) [[unlikely]]
    {
        std::printf("cannot read %s\n", file.c_str());
        return false;
    }
    const auto length = stream.tellg();
    if (length < 0) [[unlikely]]
    {
        std::printf("cannot measure %s\n", file.c_str());
        return false;
    }
    payload.resize(static_cast<std::size_t>(length));
    stream.seekg(0);
    if (!payload.empty() && !stream.read(payload.data(), static_cast<std::streamsize>(payload.size()))) [[unlikely]]
    {
        std::printf("cannot read %zu bytes of %s\n", payload.size(), file.c_str());
        return false;
    }
    return true;
}

void Print(const Outcome &outcome, const char *end, std::size_t message)
{
    if (!outcome.ok) [[unlikely]]
    {
        std::printf("%-8s FAILED: %s\n", end, outcome.failure.c_str());
    }
    else
    {
        const auto mib = static_cast<double>(outcome.bytes) / (1024.0 * 1024.0) / outcome.seconds;
        const auto gbit = static_cast<double>(outcome.bytes) * 8.0 / 1e9 / outcome.seconds;
        std::printf("%-8s %zu bytes in %.3f s = %.1f MiB/s = %.2f Gbit/s, %zu messages of %zu bytes\n", end,
                    outcome.bytes, outcome.seconds, mib, gbit, outcome.bytes / message, message);
    }
    // Line by line, as it happens: an end that has finished says so while the other
    // one may still be running, and a report held in a buffer is one nobody reads
    // when the process is stopped with Ctrl-C.
    std::fflush(stdout);
}

// A transfer that stalls says nothing by itself, and a benchmark that hangs with no
// output is one nobody can tell apart from a slow one. The watcher is what turns the
// hang into a report: how far each end got, which is the first thing anyone asks,
// and then the process ends so the next run starts from a number.
void Watch(Progress &progress, std::chrono::milliseconds budget, std::mutex &done_mutex,
           std::condition_variable &done, bool &finished)
{
    std::unique_lock lock(done_mutex);
    if (done.wait_for(lock, budget, [&finished] { return finished; }))
    {
        return;
    }
    std::printf("stalled after %lld s: sender posted %zu messages, receiver took %zu\n",
                static_cast<long long>(budget.count() / 1000), progress.sent.load(std::memory_order_relaxed),
                progress.taken.load(std::memory_order_relaxed));
    std::fflush(stdout);
    ::_exit(1);
}

// One second per ten megabytes of payload, and ten seconds over that: enough for a
// software device on a slow day, and far less than forever.
std::chrono::milliseconds Budget(std::size_t payload)
{
    return std::chrono::seconds(10) + std::chrono::milliseconds(static_cast<std::int64_t>(payload / 10000));
}
} // namespace

int main(int argc, char *argv[])
{
    CLI::App application{"RDMA throughput: one payload, one connection, both ends in this process"};

    std::string address;
    std::string device;
    std::uint16_t port = 0;
    std::size_t size = kDefaultPayload;
    std::string file;
    std::string multiplexer = "epoll";

    application.add_option("--address", address, "address of the RDMA device to run on")
        ->envname("KVSTORE_RDMA_ADDRESS")
        ->required();
    application.add_option("--rdma-device", device, "RDMA device, by name (--rdma-device siw0)")
        ->envname("KVSTORE_RDMA_DEVICE")
        ->required();
    application.add_option("--port", port, "port to listen on; 0 picks a free one")->check(CLI::Range(0, 65535));
    application.add_option("--size", size, "payload to move, in bytes (default 1 GiB)");
    application.add_option("--file", file, "read the payload from here instead of generating one");
    application.add_option("--multiplexer", multiplexer, "I/O multiplexer: epoll or io_uring")
        ->check(CLI::IsMember({"epoll", "io_uring"}));

    try
    {
        application.parse(argc, argv);
    }
    catch (const CLI::ParseError &error)
    {
        return application.exit(error);
    }

    std::vector<char> payload;
    if (!LoadPayload(file, size, payload))
    {
        return 1;
    }
    if (payload.empty()) [[unlikely]]
    {
        std::printf("there is nothing to send\n");
        return 1;
    }

    // Two managers, one per engine. The manager is single-threaded, and each end of
    // a link is driven by an engine of its own -- one per process, as they would be
    // in two servers, which is why the two ends cannot share one.
    Core::RdmaResourceManager sending_resources(device);
    Core::RdmaResourceManager receiving_resources(device);

    // The message size comes from the manager, and the receiver's chunks are what a
    // message has to fit: a bigger one would be truncated by the device rather than
    // refused, and the transfer would end in a count that does not add up.
    const auto message = sending_resources.send_memory().chunk_size();
    const auto receiving = receiving_resources.receive_memory().chunk_size();
    if (message == 0 || receiving == 0) [[unlikely]]
    {
        std::printf("the manager's chunks hold no bytes\n");
        return 1;
    }
    if (message > receiving) [[unlikely]]
    {
        std::printf("the sender's chunks are %zu bytes and the receiver's %zu: a message that does not fit is "
                    "truncated, not refused\n",
                    message, receiving);
        return 1;
    }

    const auto listening_on = port != 0 ? port : FreePort();
    if (listening_on == 0) [[unlikely]]
    {
        std::printf("no free port to listen on\n");
        return 1;
    }

    Core::RdmaAcceptor acceptor(sending_resources);
    const auto listening = acceptor.listen(Core::SocketAddress::from_v4(address, listening_on));
    if (!listening) [[unlikely]]
    {
        std::printf("cannot listen on %s:%u: %s\n", address.c_str(), listening_on, listening.error().c_str());
        return 1;
    }
    const Core::SocketAddress master = Core::SocketAddress::from_v4(address, listening_on);

    std::printf("%zu bytes as %zu messages of %zu bytes, window of %zu, %s:%u on %s, %s\n", payload.size(),
                (payload.size() + message - 1) / message, message, kWindow, address.c_str(), listening_on,
                device.c_str(), multiplexer.c_str());
    std::fflush(stdout);

    Outcome sent;
    Outcome taken;
    Progress progress;
    std::mutex done_mutex;
    std::condition_variable done;
    bool finished = false;
    std::thread watcher(Watch, std::ref(progress), Budget(payload.size()), std::ref(done_mutex), std::ref(done),
                        std::ref(finished));

    std::thread sender([&] {
        RunEngine(
            [&] {
                NBIO::RdmaAcceptChannel channel(acceptor, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
                NBIO::run(Send(channel, payload, message, sent, progress));
            },
            multiplexer, sent);
        Print(sent, "sender", message);
    });
    std::thread receiver([&] {
        RunEngine(
            [&] {
                Core::RdmaConnector connector(receiving_resources);
                NBIO::RdmaConnectChannel channel(connector, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
                NBIO::run(Receive(channel, master, receiving, taken, progress));
            },
            multiplexer, taken);
        Print(taken, "receiver", message);
    });

    // Both ends are joined before the acceptor and the managers go: every session
    // borrows them, and the sessions live on the threads.
    sender.join();
    receiver.join();
    {
        const std::lock_guard lock(done_mutex);
        finished = true;
    }
    done.notify_all();
    watcher.join();

    return sent.ok && taken.ok ? 0 : 1;
}

#else

#include <cstdio>

int main()
{
    std::printf("RDMA is only implemented on Linux\n");
    return 1;
}

#endif // defined(__linux__)
