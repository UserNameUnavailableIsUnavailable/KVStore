#include <gtest/gtest.h>

#include <Application/Server/WriteHistory.hpp>

#include <Foundation/Core/Buffer.hpp>

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace
{
using KV::WriteHistory;

KV::Command SetCommand(std::string key, std::size_t value_size)
{
    return {.type = KV::CommandType::kSet,
            .parameters = KV::SetParams{.key = std::move(key), .value = std::string(value_size, 'v')}};
}

// Reads everything the log still holds from `offset` onwards, in the sizes a
// stream would ask for it.
std::string ReadFrom(const WriteHistory &history, std::uint64_t offset)
{
    std::string text;
    std::vector<char> buffer(1024);
    while (const auto taken = history.copy(offset + text.size(), buffer))
    {
        text.append(buffer.data(), taken);
        if (taken < buffer.size())
        {
            break; // that was the end of what the log holds
        }
    }
    return text;
}
} // namespace

TEST(WriteHistoryTesting, NothingIsKeptWithoutAReplica)
{
    WriteHistory history;
    history.append(SetCommand("key", 16));

    EXPECT_EQ(history.end_offset(), 0u);
    EXPECT_EQ(history.bytes(), 0u);
    EXPECT_EQ(history.commands(), 0u);
    EXPECT_FALSE(history.recording());
    EXPECT_TRUE(history.validate());
}

TEST(WriteHistoryTesting, TheLogHoldsTheBytesTheCommandsEncodeTo)
{
    WriteHistory history;
    auto *cursor = history.attach(0);
    ASSERT_NE(cursor, nullptr);

    const auto first = SetCommand("first", 32);
    const auto second = SetCommand("second", 8);
    history.append(first);
    history.append(second);

    const std::string expected = KV::EncodeCommand(first) + KV::EncodeCommand(second);
    EXPECT_EQ(history.end_offset(), expected.size());
    EXPECT_EQ(history.bytes(), expected.size());
    EXPECT_EQ(history.commands(), 2u);
    EXPECT_EQ(history.oldest_offset(), 0u);
    EXPECT_EQ(ReadFrom(history, 0), expected);

    // Nothing has been read, so the cursor is still where it started.
    EXPECT_EQ(cursor->offset(), 0u);
    EXPECT_TRUE(history.validate());
    history.detach(cursor);
}

TEST(WriteHistoryTesting, WhatIsReadIsGivenBack)
{
    WriteHistory history;
    auto *cursor = history.attach(0);

    const auto command = SetCommand("key", 64);
    history.append(command);
    const std::string expected = KV::EncodeCommand(command);

    EXPECT_EQ(ReadFrom(history, cursor->offset()), expected);
    history.advance(*cursor, history.end_offset());

    EXPECT_EQ(history.bytes(), 0u);
    EXPECT_EQ(history.oldest_offset(), history.end_offset());
    EXPECT_EQ(cursor->offset(), expected.size());
    EXPECT_TRUE(cursor->valid());
    EXPECT_TRUE(history.validate());
    history.detach(cursor);
}

TEST(WriteHistoryTesting, OneReaderGivesBackWhatTheOtherStillNeeds)
{
    WriteHistory history;
    auto *slow = history.attach(0);
    auto *fast = history.attach(0);

    const auto command = SetCommand("key", 64);
    history.append(command);
    const std::string expected = KV::EncodeCommand(command);

    history.advance(*fast, history.end_offset());
    EXPECT_EQ(history.bytes(), expected.size()) << "the slower reader has not read it yet";
    EXPECT_EQ(history.dropped_bytes(), 0u);

    history.advance(*slow, history.end_offset());
    EXPECT_EQ(history.bytes(), 0u);
    EXPECT_TRUE(history.validate());

    history.detach(slow);
    history.detach(fast);
}

TEST(WriteHistoryTesting, AReaderBehindTheTailHoldsWhatItHasMissed)
{
    WriteHistory history;
    auto *first = history.attach(0);
    history.append(SetCommand("a", 32));
    history.append(SetCommand("b", 32));
    const std::string written = ReadFrom(history, 0);
    ASSERT_FALSE(written.empty());

    // A replica that only now starts following has to be able to read what it
    // missed, even though the reader that was there first is done with it.
    auto *late = history.attach(0);
    ASSERT_NE(late, nullptr);
    history.advance(*first, history.end_offset());
    EXPECT_EQ(history.bytes(), written.size());
    EXPECT_EQ(ReadFrom(history, late->offset()), written);

    history.advance(*late, history.end_offset());
    EXPECT_EQ(history.bytes(), 0u);
    EXPECT_TRUE(history.validate());

    history.detach(first);
    history.detach(late);
}

TEST(WriteHistoryTesting, AReaderIsCaughtUpFromWhereItStopped)
{
    WriteHistory history;
    auto *cursor = history.attach(0);

    const auto first = SetCommand("first", 32);
    const auto second = SetCommand("second", 32);
    history.append(first);
    history.advance(*cursor, history.end_offset());
    EXPECT_EQ(history.bytes(), 0u);

    history.append(second);
    EXPECT_EQ(ReadFrom(history, cursor->offset()), KV::EncodeCommand(second));

    history.advance(*cursor, history.end_offset());
    EXPECT_EQ(history.bytes(), 0u);
    EXPECT_TRUE(history.validate());
    history.detach(cursor);
}

TEST(WriteHistoryTesting, TheOffsetsGoOnWhenAReaderGoesAway)
{
    WriteHistory history;
    auto *cursor = history.attach(0);
    history.append(SetCommand("a", 32));
    const auto written = history.end_offset();
    history.advance(*cursor, written);
    history.detach(cursor);
    EXPECT_FALSE(history.recording());

    // Nobody is following, so nothing is kept -- and the offset does not go
    // back to zero either: it is what a replica that comes back asks from.
    history.append(SetCommand("b", 32));
    EXPECT_EQ(history.end_offset(), written);

    auto *renewed = history.attach(history.end_offset());
    ASSERT_NE(renewed, nullptr);
    EXPECT_EQ(renewed->offset(), written);

    history.append(SetCommand("c", 32));
    EXPECT_GT(history.end_offset(), written);
    EXPECT_EQ(ReadFrom(history, renewed->offset()), KV::EncodeCommand(SetCommand("c", 32)));
    EXPECT_TRUE(history.validate());
    history.detach(renewed);
}

TEST(WriteHistoryTesting, APositionTheLogDoesNotHoldCannotBeFollowed)
{
    WriteHistory history;
    EXPECT_EQ(history.attach(1), nullptr) << "ahead of anything that was ever written";

    auto *cursor = history.attach(0);
    history.append(SetCommand("a", 32));
    history.advance(*cursor, history.end_offset());
    EXPECT_EQ(history.attach(0), nullptr) << "behind what the log has already let go of";
    history.detach(cursor);

    auto *end = history.attach(history.end_offset());
    ASSERT_NE(end, nullptr) << "the end of the log is always a position that can be followed";
    EXPECT_TRUE(history.validate());
    history.detach(end);
}

TEST(WriteHistoryTesting, AReaderThatFallsTooFarBehindIsTold)
{
    const auto command = SetCommand("key", 40);
    const auto size = KV::EncodeCommand(command).size();
    // Room for two blocked commands: the third write passes the capacity while
    // the reader is still on the first.
    WriteHistory history(2 * size, size);

    auto *cursor = history.attach(0);
    history.append(command);
    history.append(command);
    EXPECT_TRUE(cursor->valid());
    EXPECT_EQ(history.bytes(), 2 * size);
    EXPECT_EQ(history.dropped_bytes(), 0u);

    history.append(command);
    EXPECT_FALSE(cursor->valid()) << "the reader was told rather than handed a stream with a hole in it";
    EXPECT_EQ(history.dropped_bytes(), 3 * size) << "what the reader lost is everything it had not read";
    EXPECT_EQ(history.bytes(), 0u) << "and with no reader left that can be caught up, the log lets go of the rest";
    EXPECT_EQ(history.oldest_offset(), history.end_offset());
    EXPECT_FALSE(history.recording()) << "a reader that has lost its place is not one to keep recording for";
    EXPECT_TRUE(history.validate());
    history.detach(cursor);
}

TEST(WriteHistoryTesting, ReadingStopsAtTheEndOfWhatIsWritten)
{
    WriteHistory history;
    auto *cursor = history.attach(0);
    const auto command = SetCommand("key", 64);
    history.append(command);
    const auto size = history.end_offset();

    std::vector<char> buffer(1024);
    EXPECT_EQ(history.available(size, buffer.size()), 0u);
    EXPECT_EQ(history.copy(size, buffer), 0u);
    EXPECT_EQ(history.available(0, 1), 1u) << "the reader takes what it asks for, not more";
    EXPECT_EQ(history.available(0, buffer.size()), size);

    EXPECT_TRUE(history.validate());
    history.detach(cursor);
}

TEST(WriteHistoryTesting, ACommandLargerThanABlockIsStillWhole)
{
    const auto command = SetCommand("key", 4096);
    const std::string encoded = KV::EncodeCommand(command);
    WriteHistory history(8 * encoded.size(), 64);

    auto *cursor = history.attach(0);
    history.append(command);

    EXPECT_EQ(history.bytes(), encoded.size());
    EXPECT_EQ(ReadFrom(history, 0), encoded);

    history.advance(*cursor, history.end_offset());
    EXPECT_EQ(history.bytes(), 0u);
    EXPECT_TRUE(history.validate());
    history.detach(cursor);
}

TEST(WriteHistoryTesting, TheHeadLeavesOnlyWhenNobodyHoldsIt)
{
    WriteHistory history;
    auto *slow = history.attach(0);
    history.append(SetCommand("a", 32));
    auto *fast = history.attach(history.end_offset());
    history.append(SetCommand("b", 32));
    const auto held = history.bytes();
    ASSERT_GT(held, 0u) << "two writes are not a block of their own";

    history.advance(*fast, history.end_offset());
    EXPECT_EQ(history.bytes(), held) << "the slower reader has not read the first block yet";

    history.advance(*slow, history.end_offset());
    EXPECT_EQ(history.bytes(), 0u);
    EXPECT_TRUE(history.validate());

    history.detach(slow);
    history.detach(fast);
}

TEST(WriteHistoryTesting, TheBytesAreWhatAReplicaDecodes)
{
    WriteHistory history;
    auto *cursor = history.attach(0);

    std::vector<KV::Command> written;
    for (int index = 0; index < 5; ++index)
    {
        written.push_back(SetCommand("key:" + std::to_string(index), 300));
        history.append(written.back());
    }

    const std::string stream = ReadFrom(history, 0);
    ASSERT_FALSE(stream.empty());

    // The log is a stream of commands, so the same decode and validate the AOF
    // replay and the client path use has to accept every byte of it.
    Foundation::Core::Buffer buffer(stream.size() + 512, stream.size() + 512);
    ASSERT_TRUE(buffer.write(stream.data(), stream.size()));

    std::vector<KV::Command> decoded;
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
        decoded.push_back(*validation.command);
    }

    ASSERT_EQ(decoded.size(), written.size());
    for (std::size_t index = 0; index < decoded.size(); ++index)
    {
        EXPECT_EQ(std::get<KV::SetParams>(decoded[index].parameters).key,
                  std::get<KV::SetParams>(written[index].parameters).key);
    }

    history.detach(cursor);
}

TEST(WriteHistoryTesting, TheCountsNeverDrift)
{
    const auto command = SetCommand("key", 24);
    const auto size = KV::EncodeCommand(command).size();
    WriteHistory history(8 * size, 3 * size);

    std::mt19937_64 rng{0x686973746f7279ULL};
    std::vector<WriteHistory::Cursor *> cursors;

    for (int step = 0; step < 4000; ++step)
    {
        switch (rng() % 4)
        {
        case 0:
            history.append(command);
            break;
        case 1:
            if (auto *cursor = history.attach(rng() % (history.end_offset() + 1)); cursor != nullptr)
            {
                cursors.push_back(cursor);
            }
            break;
        case 2: {
            if (cursors.empty())
            {
                break;
            }
            WriteHistory::Cursor *chosen = cursors[rng() % cursors.size()];
            if (!chosen->valid())
            {
                // What a master does with a replica that fell off the end of the
                // log: drop its place and let it synchronise from scratch.
                history.detach(chosen);
                std::erase(cursors, chosen);
                break;
            }
            const auto room = history.end_offset() - chosen->offset();
            history.advance(*chosen, chosen->offset() + (room == 0 ? 0 : rng() % (room + 1)));
            break;
        }
        default:
            if (!cursors.empty())
            {
                const auto index = rng() % cursors.size();
                history.detach(cursors[index]);
                cursors.erase(cursors.begin() + static_cast<std::ptrdiff_t>(index));
            }
            break;
        }

        ASSERT_TRUE(history.validate()) << "step " << step;
        EXPECT_EQ(history.bytes(), history.end_offset() - history.oldest_offset()) << "step " << step;
    }

    for (WriteHistory::Cursor *cursor : cursors)
    {
        history.detach(cursor);
    }

    // Nothing holds anything any more, so nothing is kept.
    EXPECT_EQ(history.bytes(), 0u);
    EXPECT_EQ(history.oldest_offset(), history.end_offset());
    EXPECT_FALSE(history.recording());
    EXPECT_TRUE(history.validate());
}
