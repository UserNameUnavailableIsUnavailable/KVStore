// One FileStream is one file descriptor with one write channel, and a channel
// drives one job at a time. Everything that shares a file -- the append-only file
// every client session writes to -- therefore relies on the channel to put the
// writes behind each other rather than let the second one write over the first
// one's job and drop its coroutine.
//
// These tests write from many coroutines at once and read the file back as a
// sequence of whole chunks: nothing may be lost, nothing may be written over, and
// a writer's own chunks have to arrive in the order it wrote them.
#include <Foundation/NBIO/NBIO.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/URingMultiplexer.hpp>

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/File.hpp>
#include <Foundation/NBIO/FileStream.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <vector>

namespace
{
constexpr std::size_t kWriters = 8;
constexpr std::size_t kChunksPerWriter = 32;
constexpr std::size_t kChunkBytes = 4096;

// Every chunk says who wrote it and which of that writer's chunks it is, so a
// chunk read back from anywhere in the file can be placed.
std::vector<char> MakeChunk(std::size_t writer, std::size_t index)
{
    std::vector<char> chunk(kChunkBytes, static_cast<char>('a' + (writer % 26)));
    const std::uint32_t header[2] = {static_cast<std::uint32_t>(writer), static_cast<std::uint32_t>(index)};
    std::memcpy(chunk.data(), header, sizeof(header));
    return chunk;
}

void ReadHeader(const char *chunk, std::uint32_t &writer, std::uint32_t &index)
{
    std::memcpy(&writer, chunk, sizeof(std::uint32_t));
    std::memcpy(&index, chunk + sizeof(std::uint32_t), sizeof(std::uint32_t));
}

// One writer's share of the load: its own chunks, one after the other, into a file
// the other writers are writing to at the same time.
Foundation::NBIO::Task<void> write_chunks(Foundation::NBIO::FileStream &file, const std::vector<std::vector<char>> &chunks,
                                          std::size_t writer)
{
    for (std::size_t index = 0; index < kChunksPerWriter; ++index)
    {
        const std::vector<char> &chunk = chunks[writer * kChunksPerWriter + index];
        (void)co_await file.write(std::span<const char>{chunk.data(), chunk.size()});
    }
}

std::vector<char> ReadFile(const std::filesystem::path &path)
{
    std::ifstream file(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

std::filesystem::path TempPath(const char *prefix)
{
    return std::filesystem::temp_directory_path() / (std::string{prefix} + std::to_string(std::random_device{}()) + ".bin");
}

// The load both backends have to carry: many writers, one file, read back as whole
// chunks in per-writer order.
void many_writers_one_file()
{
    const auto path = TempPath("kvstore-write-queue-");
    std::filesystem::remove(path);

    // The chunks outlive the run, because a queued write points at its caller's
    // bytes rather than at a copy of them.
    std::vector<std::vector<char>> chunks;
    chunks.reserve(kWriters * kChunksPerWriter);
    for (std::size_t writer = 0; writer < kWriters; ++writer)
    {
        for (std::size_t index = 0; index < kChunksPerWriter; ++index)
        {
            chunks.push_back(MakeChunk(writer, index));
        }
    }

    bool opened = false;
    Foundation::NBIO::run([&]() -> Foundation::NBIO::Task<void> {
        auto file = Foundation::NBIO::open_file(path);
        opened = static_cast<bool>(file);
        if (!opened)
        {
            co_return;
        }

        std::vector<Foundation::Async::CoroutineToken> writers;
        writers.reserve(kWriters);
        for (std::size_t writer = 0; writer < kWriters; ++writer)
        {
            writers.push_back(Foundation::NBIO::spawn(write_chunks(*file, chunks, writer)));
        }
        // The test is not finished until every writer is. A writer whose only
        // reference was dropped by an overlap never runs again, so its join is what
        // turns that into a failing (hanging) test instead of a file that quietly
        // has less in it.
        for (const Foundation::Async::CoroutineToken &writer : writers)
        {
            co_await writer;
        }
        co_return;
    }());

    ASSERT_TRUE(opened);

    const std::vector<char> bytes = ReadFile(path);
    ASSERT_EQ(bytes.size(), kWriters * kChunksPerWriter * kChunkBytes) << "every queued write has to land whole";

    // A writer's own chunks arrive in the order it wrote them, whatever order the
    // writers took turns in.
    std::vector<std::size_t> next_index(kWriters, 0);
    for (std::size_t offset = 0; offset < bytes.size(); offset += kChunkBytes)
    {
        std::uint32_t writer = 0;
        std::uint32_t index = 0;
        ReadHeader(bytes.data() + offset, writer, index);
        ASSERT_LT(writer, kWriters) << "unexpected chunk header at " << offset;
        ASSERT_LT(index, kChunksPerWriter) << "unexpected chunk header at " << offset;
        EXPECT_EQ(index, next_index[writer]) << "writer " << writer << " chunk out of order at " << offset;
        next_index[writer] = index + 1;
    }

    for (std::size_t writer = 0; writer < kWriters; ++writer)
    {
        EXPECT_EQ(next_index[writer], kChunksPerWriter) << "writer " << writer << " lost a chunk";
    }

    std::filesystem::remove(path);
}
} // namespace

TEST(FileWriteChannelTesting, WritesFromManyCoroutinesAllLand)
{
    many_writers_one_file();
}

// The completion backend batches the same writes, in one submission rather than
// one system call each. This is the engine the server runs on.
TEST(FileWriteChannelTesting, WritesFromManyCoroutinesAllLandOnURing)
{
    Foundation::NBIO::initialize(std::make_unique<Foundation::NBIO::URingMultiplexer>());
    many_writers_one_file();
}
