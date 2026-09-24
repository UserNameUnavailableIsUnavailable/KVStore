#include <gtest/gtest.h>

#if defined(__linux__)

#include <Application/Commands.hpp>
#include <Application/RESP/RESP.hpp>
#include <Application/Server/RdmaTransfer.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Core/BitmapMemory.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/RdmaAcceptor.hpp>
#include <Foundation/Core/RdmaConnector.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/NBIO.hpp>
#include <Foundation/NBIO/RdmaAcceptChannel.hpp>
#include <Foundation/NBIO/RdmaConnectChannel.hpp>
#include <Foundation/NBIO/RdmaSession.hpp>
#include <Foundation/NBIO/URingMultiplexer.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace
{
namespace Core = Foundation::Core;
namespace NBIO = Foundation::NBIO;

// One packet is one message, so this is both the chunk size and the size the
// payload is cut at.
constexpr std::size_t kPacketSize = 4096;
// What the receiver offers when it is asked to take smaller packets than the
// sender would like to send.
constexpr std::size_t kSmallPacketSize = 1024;
constexpr std::size_t kChunks = 64;

// The payload the test moves. Its packet count is what the two ends are given:
// 74 packets, which is several windows of chunks, so the sender has to wait for
// completions and pick chunks up again rather than fitting it all in the pool.
constexpr std::size_t kPayloadSize = 300000;
constexpr std::uint64_t kPayloadPackets = (kPayloadSize + kPacketSize - 1) / kPacketSize;
constexpr std::uint64_t kSmallPayloadPackets = (kPayloadSize + kSmallPacketSize - 1) / kSmallPacketSize;

// There is no way to guess which address an RDMA device answers on, so the test
// that drives one only runs where it has been named. Everything else here is a
// plain unit test.
std::string RdmaSocketAddress()
{
    if (const char *address = std::getenv("KVSTORE_RDMA_ADDRESS"); address != nullptr && *address != '\0')
    {
        return address;
    }
    return {};
}

// A port nothing else is using, asked of the kernel over TCP because an RDMA
// listener cannot ask for one.
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

void WritePattern(const std::filesystem::path &path, std::size_t size)
{
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    for (std::size_t index = 0; index < size; ++index)
    {
        file.put(static_cast<char>('a' + (index % 26)));
    }
}

std::string ReadAll(const std::filesystem::path &path)
{
    std::ifstream file(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

// What one end of the transfer reports back to the test thread.
struct Outcome
{
    bool ok{false};
    std::string failure;
};

// Runs `body` on its own engine, so the two ends are two event loops in two
// threads exactly as two servers would be.
template <typename Body> void RunEngine(Body body, Outcome &outcome)
{
    try
    {
        NBIO::initialize(std::make_unique<NBIO::URingMultiplexer>());
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

// Drives one whole transfer between two event loops in two threads, with each
// end using its own chunk size and its own pools. The bytes that arrive are what
// says the two ends agreed: a sender that ignored the offer would send packets
// too big for the receiver's chunks, and the device does not refuse those, it
// truncates them.
void TransferAcrossTheWire(const std::string &address, std::size_t sender_chunk, std::size_t receiver_chunk,
                           Outcome &sender, Outcome &receiver)
{
    const auto port = FreePort();
    ASSERT_NE(port, 0) << "could not find a free port";

    const auto directory =
        std::filesystem::temp_directory_path() / ("kvstore-file-transfer-" + std::to_string(::getpid()) + "-" +
                                                  std::to_string(sender_chunk) + "x" + std::to_string(receiver_chunk));
    std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);
    const auto source = directory / "source.rdb";
    const auto destination = directory / "destination.rdb";
    WritePattern(source, kPayloadSize);

    // The listener is opened before either thread starts, so the connecting side
    // cannot lose a race with the accepting one. It lends its protection domain
    // and its pools to every stream it hands out, which is why it outlives both
    // threads.
    Core::BitmapMemory receive_pool(sender_chunk, kChunks);
    Core::BitmapMemory send_pool(sender_chunk, kChunks);
    Core::RdmaAcceptor acceptor(std::move(receive_pool), std::move(send_pool));
    const auto listening = acceptor.listen(Core::SocketAddress::from_v4(address, port));
    ASSERT_TRUE(listening.has_value()) << listening.error();

    // The sender runs first and waits: nothing is sent until the receiver opens
    // the transfer and says how much of it it can take at once.
    std::thread sending([&] {
        RunEngine(
            [&] {
                NBIO::RdmaAcceptChannel channel(acceptor, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
                NBIO::run([&]() -> NBIO::Task<void> {
                    auto accepted = co_await channel.accept();
                    if (!accepted)
                    {
                        sender.failure = "the sender could not be admitted: " + accepted.error();
                        co_return;
                    }
                    auto &session = **accepted;
                    const auto sent = co_await KV::SendFile(session, source, sender_chunk);
                    if (sent)
                    {
                        sender.ok = *sent == kPayloadSize;
                    }
                    else
                    {
                        sender.failure = "the sender could not put the file on the wire";
                    }
                }());
            },
            sender);
    });

    std::thread receiving([&] {
        RunEngine(
            [&] {
                Core::RdmaConnector connector(Core::BitmapMemory(receiver_chunk, kChunks),
                                               Core::BitmapMemory(receiver_chunk, kChunks));
                NBIO::RdmaConnectChannel channel(connector, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
                NBIO::run([&]() -> NBIO::Task<void> {
                    auto connected = co_await channel.connect(Core::SocketAddress::from_v4(address, port));
                    if (!connected)
                    {
                        receiver.failure = "the receiver could not connect: " + connected.error();
                        co_return;
                    }
                    auto &session = **connected;
                    const auto received = co_await KV::ReceiveFile(session, destination, receiver_chunk);
                    if (received)
                    {
                        receiver.ok = received->bytes == kPayloadSize;
                    }
                    else
                    {
                        receiver.failure = "the receiver could not take the file off the wire";
                    }
                }());
            },
            receiver);
    });

    sending.join();
    receiving.join();

    EXPECT_TRUE(sender.ok) << sender.failure;
    EXPECT_TRUE(receiver.ok) << receiver.failure;
    EXPECT_EQ(ReadAll(source), ReadAll(destination));

    std::filesystem::remove_all(directory);
}

// Drives one command stream between two event loops, the way the two ends of a
// replication link would: the receiver says what the bytes are going to be, the
// sender puts commands on the wire, and the receiver takes them off again into a
// buffer of its own. Nothing here decodes anything -- that is what the kind is
// for, and the caller is the one that checks it.
void CommandsAcrossTheWire(const std::string &address, const std::string &commands, std::string &received,
                           Outcome &sender, Outcome &receiver)
{
    const auto port = FreePort();
    ASSERT_NE(port, 0) << "could not find a free port";

    Core::BitmapMemory receive_pool(kPacketSize, kChunks);
    Core::BitmapMemory send_pool(kPacketSize, kChunks);
    Core::RdmaAcceptor acceptor(std::move(receive_pool), std::move(send_pool));
    const auto listening = acceptor.listen(Core::SocketAddress::from_v4(address, port));
    ASSERT_TRUE(listening.has_value()) << listening.error();

    std::thread sending([&] {
        RunEngine(
            [&] {
                NBIO::RdmaAcceptChannel channel(acceptor, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
                NBIO::run([&]() -> NBIO::Task<void> {
                    auto accepted = co_await channel.accept();
                    if (!accepted)
                    {
                        sender.failure = "the sender could not be admitted: " + accepted.error();
                        co_return;
                    }
                    auto &session = **accepted;

                    std::size_t offset = 0;
                    const KV::PacketReader read = [&commands, &offset](std::span<char> packet) -> std::size_t {
                        const auto left = commands.size() - offset;
                        const auto take = std::min(packet.size(), left);
                        std::memcpy(packet.data(), commands.data() + offset, take);
                        offset += take;
                        return take;
                    };

                    // The receiver opens, so this side waits for it -- and what it
                    // opened the transfer for is what this side may send.
                    const auto offer = co_await KV::AcceptTransfer(session);
                    if (!offer)
                    {
                        sender.failure = "the receiver never opened a transfer";
                        co_return;
                    }
                    const auto sent = co_await SendCommands(session, *offer, commands.size(), commands.size(), read, kPacketSize);
                    if (sent)
                    {
                        sender.ok = *sent == commands.size();
                    }
                    else
                    {
                        sender.failure = "the sender could not put the commands on the wire";
                    }
                }());
            },
            sender);
    });

    std::thread receiving([&] {
        RunEngine(
            [&] {
                Core::RdmaConnector connector(Core::BitmapMemory(kPacketSize, kChunks),
                                               Core::BitmapMemory(kPacketSize, kChunks));
                NBIO::RdmaConnectChannel channel(connector, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
                NBIO::run([&]() -> NBIO::Task<void> {
                    auto connected = co_await channel.connect(Core::SocketAddress::from_v4(address, port));
                    if (!connected)
                    {
                        receiver.failure = "the receiver could not connect: " + connected.error();
                        co_return;
                    }
                    auto &session = **connected;
                    const KV::PacketWriter write = [&received](std::span<const char> packet) -> bool {
                        received.append(packet.data(), packet.size());
                        return true;
                    };
                    const auto got = co_await KV::ReceiveCommands(session, 0, write, kPacketSize);
                    if (got)
                    {
                        receiver.ok = got->bytes == received.size();
                    }
                    else
                    {
                        receiver.failure = "the receiver could not take the commands off the wire";
                    }
                }());
            },
            receiver);
    });

    sending.join();
    receiving.join();

    EXPECT_TRUE(sender.ok) << sender.failure;
    EXPECT_TRUE(receiver.ok) << receiver.failure;
}

// The level the logs run at has to be reachable without a rebuild: a transfer
// that stalls has nothing else to say for itself.
void SetLogLevelFromEnvironment()
{
    if (const char *level = std::getenv("KVSTORE_LOG_LEVEL"); level != nullptr && *level != '\0')
    {
        spdlog::set_level(spdlog::level::from_str(level));
    }
}
} // namespace

TEST(RdmaTransferTesting, PacketCountRoundsUp)
{
    EXPECT_EQ(KV::PacketCount(1, 4096), 1u);
    EXPECT_EQ(KV::PacketCount(4096, 4096), 1u);
    EXPECT_EQ(KV::PacketCount(4097, 4096), 2u);
    EXPECT_EQ(KV::PacketCount(kPayloadSize, kPacketSize), kPayloadPackets);
}

TEST(RdmaTransferTesting, NothingIsZeroPackets)
{
    // Zero is what "there is nothing to send" looks like, so it must not be a
    // count an actual payload can have.
    EXPECT_EQ(KV::PacketCount(0, 4096), 0u);
    EXPECT_EQ(KV::PacketCount(4096, 0), 0u);
}

TEST(RdmaTransferTesting, OpeningCarriesTheKindAndWhatTheReceiverCanTake)
{
    // The two kinds are different lengths on the wire, so an opening is one of
    // two sizes and the receiver is told which of them it is looking at.
    const auto snapshot =
        EncodeOpen(KV::TransferOffer{.kind = KV::PayloadKind::kSnapshot, .chunk_size = 4096, .num_chunks = 16});
    const auto commands =
        EncodeOpen(KV::TransferOffer{.kind = KV::PayloadKind::kCommands, .chunk_size = 1024, .num_chunks = 4});
    ASSERT_EQ(snapshot.size(), OpenMessageBytes(KV::PayloadKind::kSnapshot));
    ASSERT_EQ(commands.size(), OpenMessageBytes(KV::PayloadKind::kCommands));
    EXPECT_NE(snapshot.size(), commands.size());

    KV::TransferOffer offer;
    ASSERT_TRUE(DecodeOpen(snapshot, offer));
    EXPECT_EQ(offer.kind, KV::PayloadKind::kSnapshot);
    EXPECT_EQ(offer.chunk_size, 4096u);
    EXPECT_EQ(offer.num_chunks, 16u);

    ASSERT_TRUE(DecodeOpen(commands, offer));
    EXPECT_EQ(offer.kind, KV::PayloadKind::kCommands);
    EXPECT_EQ(offer.chunk_size, 1024u);
    EXPECT_EQ(offer.num_chunks, 4u);
}

TEST(RdmaTransferTesting, TheKindWordIsWhatSaysWhatThePayloadIs)
{
    KV::TransferOffer offer;
    EXPECT_FALSE(DecodeOpen("", offer));
    EXPECT_FALSE(DecodeOpen("DATA", offer));

    // A message of another kind is not an opening, whatever its size.
    EXPECT_FALSE(DecodeOpen(KV::EncodePlanHeader(1024, 74, 0), offer));

    auto message = EncodeOpen(KV::TransferOffer{.kind = KV::PayloadKind::kSnapshot, .chunk_size = 4096, .num_chunks = 16});
    message[0] = 'N';
    EXPECT_FALSE(DecodeOpen(message, offer));
    EXPECT_FALSE(DecodeOpen(message.substr(0, message.size() - 1), offer));

    // The word has to end: a message where the separator is a letter is not the
    // kind it looks like, and reading it as one would put the fields behind it at
    // the wrong offsets.
    auto unseparated = EncodeOpen(KV::TransferOffer{.kind = KV::PayloadKind::kCommands, .chunk_size = 4096, .num_chunks = 16});
    unseparated[WireWord(KV::PayloadKind::kCommands).size()] = 'X';
    EXPECT_FALSE(DecodeOpen(unseparated, offer));

    // And the two kinds are not interchangeable: a message the length of one is
    // not read as the other, so the fields are never taken from a word that did
    // not say where they are.
    auto mismatched = EncodeOpen(KV::TransferOffer{.kind = KV::PayloadKind::kCommands, .chunk_size = 4096, .num_chunks = 16});
    mismatched.resize(OpenMessageBytes(KV::PayloadKind::kSnapshot));
    EXPECT_FALSE(DecodeOpen(mismatched, offer));
}

TEST(RdmaTransferTesting, TheMessagesAreToldApart)
{
    // Every operation starts with four letters and a number, so nothing but the
    // word itself can say which one arrived.
    const auto opening =
        EncodeOpen(KV::TransferOffer{.kind = KV::PayloadKind::kSnapshot, .chunk_size = 4096, .num_chunks = 16});
    std::uint32_t chunk = 0;
    std::uint32_t written = 0;
    std::uint32_t packets = 0;
    std::uint64_t count = 0;
    std::uint64_t offset = 0;
    EXPECT_FALSE(KV::DecodePlanHeader(opening, chunk, packets, offset));
    EXPECT_FALSE(KV::DecodeCreditHeader(opening, written));
    EXPECT_FALSE(KV::DecodeDoneHeader(opening, count));

    const auto plan = KV::EncodePlanHeader(1024, 74, 0);
    KV::TransferOffer offer;
    EXPECT_FALSE(KV::DecodeOpen(plan, offer));
    EXPECT_FALSE(KV::DecodeCreditHeader(plan, written));
    EXPECT_FALSE(KV::DecodeDoneHeader(plan, count));
}

TEST(RdmaTransferTesting, PlanCarriesWhatTheSenderDecided)
{
    // The count is 32 bits because a credit is: a payload whose packets did not
    // fit that width could be announced and then never reported. The offset is 64,
    // because a log is read for as long as a server runs.
    for (const auto packets : {std::uint32_t{1}, static_cast<std::uint32_t>(kSmallPayloadPackets),
                               std::uint32_t{0x1234'5678}})
    {
        const auto message = KV::EncodePlanHeader(1024, packets, 0x1122'3344'5566'7788ULL);
        ASSERT_EQ(message.size(), KV::kPlanMessageBytes);

        std::uint32_t chunk = 0;
        std::uint32_t decoded = 0;
        std::uint64_t offset = 0;
        ASSERT_TRUE(KV::DecodePlanHeader(message, chunk, decoded, offset));
        EXPECT_EQ(chunk, 1024u);
        EXPECT_EQ(decoded, packets);
        EXPECT_EQ(offset, 0x1122'3344'5566'7788ULL);
    }

    std::uint32_t chunk = 0;
    std::uint32_t packets = 0;
    std::uint64_t offset = 0;
    EXPECT_FALSE(KV::DecodePlanHeader("", chunk, packets, offset));
    EXPECT_FALSE(KV::DecodePlanHeader(std::string(KV::kPlanMessageBytes + 1, '\0'), chunk, packets, offset));
}

TEST(RdmaTransferTesting, CreditCarriesTheCountWritten)
{
    for (const auto written : {std::uint32_t{1}, std::uint32_t{74}, std::uint32_t{0x1234'5678}})
    {
        const auto message = KV::EncodeCreditHeader(written);
        ASSERT_EQ(message.size(), KV::kCreditMessageBytes);

        std::uint32_t decoded = 0;
        ASSERT_TRUE(KV::DecodeCreditHeader(message, decoded));
        EXPECT_EQ(decoded, written);
    }

    std::uint32_t written = 0;
    EXPECT_FALSE(KV::DecodeCreditHeader("", written));
    EXPECT_FALSE(KV::DecodeCreditHeader(std::string(KV::kCreditMessageBytes + 1, '\0'), written));
}

TEST(RdmaTransferTesting, DoneCarriesTheBytesWritten)
{
    const auto message = KV::EncodeDoneHeader(kPayloadSize);
    ASSERT_EQ(message.size(), KV::kDoneMessageBytes);

    std::uint64_t bytes = 0;
    ASSERT_TRUE(KV::DecodeDoneHeader(message, bytes));
    EXPECT_EQ(bytes, kPayloadSize);

    EXPECT_FALSE(KV::DecodeDoneHeader("", bytes));
    EXPECT_FALSE(KV::DecodeDoneHeader(std::string(KV::kDoneMessageBytes + 1, '\0'), bytes));
}

TEST(RdmaTransferTesting, TheNumbersAreInNetworkOrder)
{
    // The two ends need not be the same machine, and what crosses is not only the
    // payload: every number has to mean the same thing at both of them. So it
    // goes out the way every protocol since TCP's own header sends one -- most
    // significant byte first -- rather than the way either machine happens to
    // hold it in a register.
    const auto message = KV::EncodePlanHeader(0x11223344, 0x55667788, 0x99AABBCCDDEEFF00ULL);
    const auto *fields = message.data() + KV::kPlanHeader.size() + 1;
    EXPECT_EQ(static_cast<unsigned char>(fields[0]), 0x11u);
    EXPECT_EQ(static_cast<unsigned char>(fields[3]), 0x44u);
    EXPECT_EQ(static_cast<unsigned char>(fields[4]), 0x55u);
    EXPECT_EQ(static_cast<unsigned char>(fields[7]), 0x88u);
    EXPECT_EQ(static_cast<unsigned char>(fields[8]), 0x99u);
    EXPECT_EQ(static_cast<unsigned char>(fields[15]), 0x00u);
}

TEST(RdmaTransferTesting, TheOfferIsWhatTheReceiverCanHold)
{
    const auto offer = ReceiverOffer(KV::PayloadKind::kCommands, kSmallPacketSize);
    EXPECT_EQ(offer.kind, KV::PayloadKind::kCommands);
    EXPECT_EQ(offer.chunk_size, kSmallPacketSize);
    EXPECT_EQ(offer.num_chunks, Core::RdmaStream::kReceiveChunks);

    // Every packet the sender may have in flight has to land in a receive, so
    // the window it is allowed can never be wider than what was offered -- and
    // two ends posting the same number of receives can use all of them: each
    // packet is reported exactly once, so the reports never outnumber the
    // packets and the confirmation is not one more message on top.
    EXPECT_LE(NegotiatedWindow(offer), offer.num_chunks);
    EXPECT_EQ(NegotiatedWindow(offer), Core::RdmaStream::kReceiveChunks);
}

TEST(RdmaTransferTesting, TheWindowIsTheSmallerOfTheTwoReceiveCounts)
{
    // Reports come back as messages and land in the *sender's* receive queue, so
    // a receiver offering more packets than the sender has receives would fill
    // that queue and take the connection down from the sender's side. Nothing is
    // held back, though: the reports mirror the packets one for one, so the one
    // window covers what both queues hold between them.
    const auto generous = KV::TransferOffer{.kind = KV::PayloadKind::kSnapshot, .chunk_size = 4096, .num_chunks = 4096};
    EXPECT_EQ(NegotiatedWindow(generous), Core::RdmaStream::kReceiveChunks);

    // A receiver that offers less is the one that decides.
    const auto modest = KV::TransferOffer{.kind = KV::PayloadKind::kSnapshot, .chunk_size = 4096, .num_chunks = 2};
    EXPECT_EQ(NegotiatedWindow(modest), 2u);

    EXPECT_EQ(NegotiatedWindow(KV::TransferOffer{.kind = KV::PayloadKind::kSnapshot, .chunk_size = 4096, .num_chunks = 0}),
              0u);

    // What the bytes are makes no difference to how many of them may be in
    // flight: the window is about messages, and both kinds are messages.
    const auto commands = KV::TransferOffer{.kind = KV::PayloadKind::kCommands, .chunk_size = 4096, .num_chunks = 2};
    EXPECT_EQ(NegotiatedWindow(commands), NegotiatedWindow(modest));
}

TEST(RdmaTransferTesting, SendsAFileAcrossTheWire)
{
    SetLogLevelFromEnvironment();

    const std::string address = RdmaSocketAddress();
    if (address.empty())
    {
        GTEST_SKIP() << "set KVSTORE_RDMA_ADDRESS to the address of an RDMA device to run this";
    }

    Outcome sender;
    Outcome receiver;
    TransferAcrossTheWire(address, kPacketSize, kPacketSize, sender, receiver);
    ASSERT_EQ(kPayloadPackets, KV::PacketCount(kPayloadSize, kPacketSize));
}

TEST(RdmaTransferTesting, SendsWithTheSmallerOfTheTwoChunkSizes)
{
    // The two ends are configured apart from each other, so the receiver's offer
    // is the smaller of the two and it is the one that decides how the payload is
    // cut. If the sender sent its own chunk size anyway the packets would be
    // truncated rather than refused, and the file would arrive wrong.
    SetLogLevelFromEnvironment();

    const std::string address = RdmaSocketAddress();
    if (address.empty())
    {
        GTEST_SKIP() << "set KVSTORE_RDMA_ADDRESS to the address of an RDMA device to run this";
    }

    Outcome sender;
    Outcome receiver;
    TransferAcrossTheWire(address, kPacketSize, kSmallPacketSize, sender, receiver);
    ASSERT_EQ(kSmallPayloadPackets, KV::PacketCount(kPayloadSize, kSmallPacketSize));
}

TEST(RdmaTransferTesting, SendsCommandsAcrossTheWire)
{
    // The other kind, and the one a replica applies rather than writes down: what
    // arrives has to be the commands that were sent, and the receiver has to be
    // able to decode them with the same reader the command path uses -- which is
    // the whole reason the kind is on the wire at all.
    SetLogLevelFromEnvironment();

    const std::string address = RdmaSocketAddress();
    if (address.empty())
    {
        GTEST_SKIP() << "set KVSTORE_RDMA_ADDRESS to the address of an RDMA device to run this";
    }

    // Values long enough that a packet ends in the middle of a command, so the
    // receiver has to hold a half-decoded one -- the normal case for this kind
    // rather than an error.
    std::string commands;
    std::vector<std::string> keys;
    for (int index = 0; index < 8; ++index)
    {
        keys.push_back("key:" + std::to_string(index));
        const KV::Command command{.type = KV::CommandType::kSet,
                                  .parameters = KV::SetParams{.key = keys.back(), .value = std::string(3000, 'v')}};
        commands += KV::EncodeCommand(command);
    }

    std::string received;
    Outcome sender;
    Outcome receiver;
    CommandsAcrossTheWire(address, commands, received, sender, receiver);

    ASSERT_EQ(received, commands);

    // What arrived is a run of commands, so the same decode-and-validate the AOF
    // replay and the client path use has to accept every one of them.
    Core::Buffer buffer(received.size() + 512, received.size() + 512);
    ASSERT_TRUE(buffer.write(received.data(), received.size()));
    std::vector<KV::Command> applied;
    while (!buffer.is_empty())
    {
        auto decoder = RESP::Decode(buffer);
        while (!decoder.done())
        {
            decoder.resume();
        }
        ASSERT_EQ(decoder.status(), RESP::DecodeStatus::kComplete);
        ASSERT_TRUE(decoder.result().object.has_value());
        const auto validation = KV::ValidateCommand(*decoder.result().object);
        ASSERT_TRUE(validation.command.has_value());
        applied.push_back(*validation.command);
    }

    ASSERT_EQ(applied.size(), keys.size());
    for (std::size_t index = 0; index < keys.size(); ++index)
    {
        const auto &set = std::get<KV::SetParams>(applied[index].parameters);
        EXPECT_EQ(set.key, keys[index]);
        EXPECT_EQ(set.value.size(), 3000u);
    }
}

TEST(RdmaTransferTesting, ASnapshotIsRefusedWhereCommandsWereAskedFor)
{
    // The other half of putting the kind on the wire: a sender that has the wrong
    // payload refuses rather than sending bytes the far end has already said it
    // cannot use. With the sender refusing, the link ends, so the receiver finds
    // out instead of waiting for a plan that will never come.
    SetLogLevelFromEnvironment();

    const std::string address = RdmaSocketAddress();
    if (address.empty())
    {
        GTEST_SKIP() << "set KVSTORE_RDMA_ADDRESS to the address of an RDMA device to run this";
    }

    const auto port = FreePort();
    ASSERT_NE(port, 0) << "could not find a free port";

    const auto directory =
        std::filesystem::temp_directory_path() / ("kvstore-transfer-kind-" + std::to_string(::getpid()));
    std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);
    const auto source = directory / "source.rdb";
    WritePattern(source, kPayloadSize);

    Core::BitmapMemory receive_pool(kPacketSize, kChunks);
    Core::BitmapMemory send_pool(kPacketSize, kChunks);
    Core::RdmaAcceptor acceptor(std::move(receive_pool), std::move(send_pool));
    const auto listening = acceptor.listen(Core::SocketAddress::from_v4(address, port));
    ASSERT_TRUE(listening.has_value()) << listening.error();

    Outcome sender;
    Outcome receiver;
    std::string received;

    std::thread sending([&] {
        RunEngine(
            [&] {
                NBIO::RdmaAcceptChannel channel(acceptor, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
                NBIO::run([&]() -> NBIO::Task<void> {
                    auto accepted = co_await channel.accept();
                    if (!accepted)
                    {
                        sender.failure = "the snapshot sender could not be admitted: " + accepted.error();
                        co_return;
                    }
                    auto &session = **accepted;
                    const auto sent = co_await KV::SendFile(session, source, kPacketSize);
                    sender.ok = !sent.has_value();
                    sender.failure = "the sender put a snapshot on a transfer opened for commands";
                }());
            },
            sender);
    });

    std::thread receiving([&] {
        RunEngine(
            [&] {
                Core::RdmaConnector connector(Core::BitmapMemory(kPacketSize, kChunks),
                                               Core::BitmapMemory(kPacketSize, kChunks));
                NBIO::RdmaConnectChannel channel(connector, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
                NBIO::run([&]() -> NBIO::Task<void> {
                    auto connected = co_await channel.connect(Core::SocketAddress::from_v4(address, port));
                    if (!connected)
                    {
                        receiver.failure = "the receiver could not connect: " + connected.error();
                        co_return;
                    }
                    auto &session = **connected;
                    const KV::PacketWriter write = [&received](std::span<const char> packet) -> bool {
                        received.append(packet.data(), packet.size());
                        return true;
                    };
                    const auto got = co_await KV::ReceiveCommands(session, 0, write, kPacketSize);
                    receiver.ok = !got.has_value();
                    receiver.failure = "the receiver took a payload the sender should have refused";
                }());
            },
            receiver);
    });

    sending.join();
    receiving.join();

    EXPECT_TRUE(sender.ok) << sender.failure;
    EXPECT_TRUE(receiver.ok) << receiver.failure;
    EXPECT_TRUE(received.empty());

    std::filesystem::remove_all(directory);
}

#endif // defined(__linux__)
