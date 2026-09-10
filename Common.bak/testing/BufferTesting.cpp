#include <gtest/gtest.h>

#include <cstring>
#include <forward_list>
#include <list>
#include <memory_resource>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "::Foundation::Buffer.hpp"

namespace
{
// ::Foundation::Buffer takes a (pointer, size) pair or an iterator pair, never a view, so
// these spell out the range once for the tests that only care about content.
void AppendText(KV::Foundation::Buffer& buffer, std::string_view text)
{
    buffer.Append(text.data(), text.size());
}

void PrependText(KV::Foundation::Buffer& buffer, std::string_view text)
{
    buffer.Prepend(text.data(), text.size());
}

// Simulate a kernel read: take the room the buffer offers, fill it, publish it.
// This is deliberately not Append -- it exercises the Reserve/Commit pair that
// recv and io_uring actually use.
void Receive(KV::Foundation::Buffer& buffer, std::string_view payload)
{
    const std::span<char> room = buffer.Reserve(payload.size());
    ASSERT_GE(room.size(), payload.size());
    std::memcpy(room.data(), payload.data(), payload.size());
    buffer.Commit(payload.size());
}

// A resource that counts allocations, so a test can assert that a workload
// stays on one allocation instead of merely asserting on Capacity().
class CountingResource final : public std::pmr::memory_resource
{
public:
    std::size_t GetAllocationCount() const noexcept { return allocations_; }
    std::size_t GetLiveBytes() const noexcept { return live_bytes_; }

private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override
    {
        ++allocations_;
        live_bytes_ += bytes;
        return std::pmr::new_delete_resource()->allocate(bytes, alignment);
    }

    void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) override
    {
        live_bytes_ -= bytes;
        std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override
    {
        return this == &other;
    }

    std::size_t allocations_ = 0;
    std::size_t live_bytes_ = 0;
};
} // namespace

// =============================================================================
// Construction and invariants
// =============================================================================

TEST(::Foundation::BufferTesting, StartsEmptyWithTheDefaultCapacity)
{
    const KV::Foundation::Buffer buffer;

    EXPECT_TRUE(buffer.Empty());
    EXPECT_EQ(buffer.ReadableSize(), 0u);
    EXPECT_EQ(buffer.Capacity(), KV::Foundation::Buffer::kDefaultCapacity);
    EXPECT_EQ(buffer.WritableSize(), KV::Foundation::Buffer::kDefaultCapacity);
    EXPECT_EQ(buffer.PrependableSize(), 0u);
    EXPECT_EQ(buffer.StringView(), "");
}

TEST(::Foundation::BufferTesting, HonoursAnExplicitInitialCapacity)
{
    const KV::Foundation::Buffer buffer(64);

    EXPECT_EQ(buffer.Capacity(), 64u);
    EXPECT_EQ(buffer.WritableSize(), 64u);
    EXPECT_TRUE(buffer.Empty());
}

TEST(::Foundation::BufferTesting, AZeroCapacity::Foundation::BufferIsUsable)
{
    // std::size_t{0} rather than 0: a literal 0 is also a null pointer constant,
    // so it would be ambiguous against ::Foundation::Buffer(memory_resource*) -- the same trap
    // std::vector<T*>(0) has.
    KV::Foundation::Buffer buffer(std::size_t {0});
    ASSERT_EQ(buffer.Capacity(), 0u);

    Receive(buffer, "payload");

    EXPECT_EQ(buffer.StringView(), "payload");
    EXPECT_GE(buffer.Capacity(), 7u);
}

TEST(::Foundation::BufferTesting, TheThreeRegionsAlwaysTileTheCapacity)
{
    KV::Foundation::Buffer buffer(32);

    Receive(buffer, "0123456789");
    EXPECT_EQ(buffer.PrependableSize() + buffer.ReadableSize() + buffer.WritableSize(),
        buffer.Capacity());

    buffer.Consume(4);

    // Consuming does not shift bytes: the four bytes moved from readable to
    // prependable, and nothing else changed.
    EXPECT_EQ(buffer.PrependableSize(), 4u);
    EXPECT_EQ(buffer.ReadableSize(), 6u);
    EXPECT_EQ(buffer.WritableSize(), 22u);
}

// =============================================================================
// Reserve and Commit: the kernel-fill path
// =============================================================================

TEST(::Foundation::BufferTesting, ReserveHandsBackTheWholeTailNotJustTheRequest)
{
    KV::Foundation::Buffer buffer(128);

    // A recv should be allowed to take everything available in one syscall, so
    // asking for 10 must not cap the span at 10.
    const std::span<char> room = buffer.Reserve(10);

    EXPECT_EQ(room.size(), 128u);
}

TEST(::Foundation::BufferTesting, CommitPublishesExactlyWhatWasWritten)
{
    KV::Foundation::Buffer buffer(64);
    const std::span<char> room = buffer.Reserve(16);

    std::memcpy(room.data(), "abcdefghij", 10);
    buffer.Commit(10); // a short read: room was 64, only 10 arrived

    EXPECT_EQ(buffer.ReadableSize(), 10u);
    EXPECT_EQ(buffer.StringView(), "abcdefghij");
}

TEST(::Foundation::BufferTesting, CommittingNothingLeavesThe::Foundation::BufferEmpty)
{
    KV::Foundation::Buffer buffer(64);
    buffer.Reserve(16);

    buffer.Commit(0); // recv returned 0: peer closed

    EXPECT_TRUE(buffer.Empty());
    EXPECT_EQ(buffer.Capacity(), 64u);
}

TEST(::Foundation::BufferTesting, CommitIsClampedToTheWritableRoom)
{
    KV::Foundation::Buffer buffer(16);
    buffer.Reserve(4);

    // A miscounted result must not push the cursor past the storage.
    buffer.Commit(1000);

    EXPECT_EQ(buffer.ReadableSize(), 16u);
    EXPECT_EQ(buffer.WritableSize(), 0u);
}

TEST(::Foundation::BufferTesting, SuccessiveReceivesAccumulate)
{
    KV::Foundation::Buffer buffer(64);

    Receive(buffer, "*2\r\n");
    Receive(buffer, "$3\r\nGET\r\n");
    Receive(buffer, "$3\r\nkey\r\n");

    // A request split across three reads has to read back as one contiguous view.
    EXPECT_EQ(buffer.StringView(), "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
}

// =============================================================================
// Append
// =============================================================================

TEST(::Foundation::BufferTesting, AppendCopiesFromAPointerAndSize)
{
    KV::Foundation::Buffer buffer;
    const std::string_view response = "+OK\r\n";

    buffer.Append(response.data(), response.size());

    EXPECT_EQ(buffer.StringView(), "+OK\r\n");
    EXPECT_EQ(buffer.ReadableSize(), 5u);
}

TEST(::Foundation::BufferTesting, AppendCopiesFromAContiguousIteratorPair)
{
    KV::Foundation::Buffer buffer;
    const std::string payload = "contiguous";
    const std::vector<char> characters {'/', 'v', 'e', 'c'};

    buffer.Append(payload.begin(), payload.end());
    buffer.Append(characters.begin(), characters.end());

    // A contiguous range takes the memcpy path.
    EXPECT_EQ(buffer.StringView(), "contiguous/vec");
}

TEST(::Foundation::BufferTesting, AppendCopiesFromANonContiguousIteratorPair)
{
    KV::Foundation::Buffer buffer;
    const std::list<char> listed {'l', 'i', 's', 't'};
    const std::forward_list<char> forward {'-', 'f', 'w', 'd'};

    // No pointer to memcpy from, so these take the element-wise path.
    buffer.Append(listed.begin(), listed.end());
    buffer.Append(forward.begin(), forward.end());

    EXPECT_EQ(buffer.StringView(), "list-fwd");
}

TEST(::Foundation::BufferTesting, AppendAcceptsEveryOneByteType)
{
    KV::Foundation::Buffer buffer;
    const std::vector<unsigned char> unsigned_bytes {'u', 'n'};
    const std::vector<signed char> signed_bytes {'s', 'i'};
    const std::vector<std::uint8_t> u8_bytes {0x75, 0x38}; // 'u', '8'
    const std::vector<char8_t> c8_bytes {u8'c', u8'8'};

    buffer.Append(unsigned_bytes.begin(), unsigned_bytes.end());
    buffer.Append(signed_bytes.begin(), signed_bytes.end());
    buffer.Append(u8_bytes.begin(), u8_bytes.end());
    buffer.Append(c8_bytes.begin(), c8_bytes.end());

    // The byte concept is sizeof(T) == 1, so uint8_t and char8_t qualify without
    // ever being named in the concept.
    EXPECT_EQ(buffer.StringView(), "unsiu8c8");
}

TEST(::Foundation::BufferTesting, AppendQueuesBehindDataThatHasNotDrained)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, "+FIRST\r\n");

    // Only part of the first response made it out of the socket.
    buffer.Consume(3);
    ASSERT_EQ(buffer.StringView(), "RST\r\n");

    // Queueing the next response must not discard the unflushed remainder. This
    // is the whole reason Append exists rather than an assign-style API.
    AppendText(buffer, "+SECOND\r\n");

    EXPECT_EQ(buffer.StringView(), "RST\r\n+SECOND\r\n");
}

TEST(::Foundation::BufferTesting, AppendingNothingIsANoOp)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, "data");
    const std::string empty;
    const std::list<char> empty_list;

    buffer.Append(nullptr, 0);
    buffer.Append(empty.begin(), empty.end());
    buffer.Append(empty_list.begin(), empty_list.end());

    EXPECT_EQ(buffer.StringView(), "data");
}

TEST(::Foundation::BufferTesting, AppendGrowsPastTheInitialCapacity)
{
    KV::Foundation::Buffer buffer(8);
    const std::string payload(1000, 'x');

    AppendText(buffer, payload);

    EXPECT_EQ(buffer.ReadableSize(), 1000u);
    EXPECT_EQ(buffer.StringView(), payload);
    EXPECT_GE(buffer.Capacity(), 1000u);
}

// =============================================================================
// Prepend
// =============================================================================

TEST(::Foundation::BufferTesting, PrependWritesInFrontOfTheReadableBytes)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, "payload");

    PrependText(buffer, "header:");

    EXPECT_EQ(buffer.StringView(), "header:payload");
}

TEST(::Foundation::BufferTesting, PrependReusesTheConsumedPrefixWithoutShifting)
{
    KV::Foundation::Buffer buffer(64);
    Receive(buffer, "0123456789");
    buffer.Consume(6); // six bytes of prependable room opened up
    ASSERT_EQ(buffer.PrependableSize(), 6u);
    const char* live = buffer.Readable().data();

    PrependText(buffer, "ABC");

    // The gap the read cursor left behind is exactly what Prepend writes into,
    // so this is a cursor move: the existing bytes never budge.
    EXPECT_EQ(buffer.Readable().data(), live - 3);
    EXPECT_EQ(buffer.StringView(), "ABC6789");
    EXPECT_EQ(buffer.PrependableSize(), 3u);
    EXPECT_EQ(buffer.Capacity(), 64u);
}

TEST(::Foundation::BufferTesting, PrependMakesRoomWhenTheFrontIsFull)
{
    KV::Foundation::Buffer buffer(64);
    Receive(buffer, "payload");
    ASSERT_EQ(buffer.PrependableSize(), 0u);

    // Nothing has been consumed, so the live bytes have to slide right to open
    // the gap. The content still has to come out in order.
    PrependText(buffer, "$7\r\n");

    EXPECT_EQ(buffer.StringView(), "$7\r\npayload");
    EXPECT_EQ(buffer.Capacity(), 64u); // shifted in place, no reallocation
}

TEST(::Foundation::BufferTesting, PrependGrowsWhenNeitherendHasRoom)
{
    KV::Foundation::Buffer buffer(8);
    Receive(buffer, "12345678"); // completely full
    ASSERT_EQ(buffer.WritableSize(), 0u);
    ASSERT_EQ(buffer.PrependableSize(), 0u);

    PrependText(buffer, "header");

    EXPECT_EQ(buffer.StringView(), "header12345678");
    EXPECT_GE(buffer.Capacity(), 14u);
}

TEST(::Foundation::BufferTesting, PrependOnAnEmpty::Foundation::BufferJustWrites)
{
    KV::Foundation::Buffer buffer;

    PrependText(buffer, "alone");

    EXPECT_EQ(buffer.StringView(), "alone");
}

TEST(::Foundation::BufferTesting, PrependingNothingIsANoOp)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, "data");
    const std::string empty;

    buffer.Prepend(nullptr, 0);
    buffer.Prepend(empty.begin(), empty.end());

    EXPECT_EQ(buffer.StringView(), "data");
    EXPECT_EQ(buffer.PrependableSize(), 0u);
}

TEST(::Foundation::BufferTesting, SuccessivePrependsStackOutward)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, "core");

    PrependText(buffer, "inner-");
    PrependText(buffer, "outer-");

    // Each prepend goes in front of the previous one, so the last call ends up
    // leftmost -- the order a nested framing header needs.
    EXPECT_EQ(buffer.StringView(), "outer-inner-core");
}

TEST(::Foundation::BufferTesting, PrependAcceptsIteratorPairs)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, "body");
    const std::string contiguous = "B:";
    const std::list<char> listed {'A', ':'};

    buffer.Prepend(contiguous.begin(), contiguous.end());
    buffer.Prepend(listed.begin(), listed.end());

    EXPECT_EQ(buffer.StringView(), "A:B:body");
}

TEST(::Foundation::BufferTesting, PrependIsBinarySafe)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, "payload");
    const std::string header {'\x00', '\xff', '\r', '\n'};

    buffer.Prepend(header.data(), header.size());

    ASSERT_EQ(buffer.ReadableSize(), 11u);
    EXPECT_EQ(static_cast<unsigned char>(buffer.StringView()[1]), 0xffu);
    EXPECT_EQ(buffer.StringView().substr(4), "payload");
}

TEST(::Foundation::BufferTesting, FramingAPayloadWhoseLengthIsOnlyKnownAfterwards)
{
    // The motivating case: the header depends on the payload, so the payload has
    // to be staged first. Prepend avoids buffering it elsewhere just to measure
    // it and then copying it in behind the header.
    KV::Foundation::Buffer buffer;
    const std::string payload = "line1\r\nline2";

    AppendText(buffer, payload);
    const std::string header = "$" + std::to_string(buffer.ReadableSize()) + "\r\n";
    PrependText(buffer, header);
    AppendText(buffer, "\r\n");

    EXPECT_EQ(buffer.StringView(), "$12\r\nline1\r\nline2\r\n");
}

// =============================================================================
// ReservePrepend
// =============================================================================

TEST(::Foundation::BufferTesting, ReservePrependOpensTheGapUpFront)
{
    KV::Foundation::Buffer buffer(64);

    buffer.ReservePrepend(8);

    EXPECT_EQ(buffer.PrependableSize(), 8u);
    EXPECT_TRUE(buffer.Empty());
    EXPECT_EQ(buffer.WritableSize(), 56u);
}

TEST(::Foundation::BufferTesting, AReservedGapMakesPrependACursorMove)
{
    KV::Foundation::Buffer buffer(64);
    buffer.ReservePrepend(8);
    AppendText(buffer, "payload");
    const char* live = buffer.Readable().data();

    buffer.ReservePrepend(8); // already satisfied
    PrependText(buffer, "$7\r\n");

    // The payload was never shifted, because the room was already there.
    EXPECT_EQ(buffer.Readable().data(), live - 4);
    EXPECT_EQ(buffer.StringView(), "$7\r\npayload");
}

TEST(::Foundation::BufferTesting, AReservedGapSurvivesDraining)
{
    KV::Foundation::Buffer buffer(64);
    buffer.ReservePrepend(8);

    AppendText(buffer, "payload");
    buffer.Consume(buffer.ReadableSize());

    // The reservation is sticky: draining rewinds to the gap rather than to
    // zero, so it is worth asking for once at setup instead of per message.
    EXPECT_TRUE(buffer.Empty());
    EXPECT_EQ(buffer.PrependableSize(), 8u);
}

TEST(::Foundation::BufferTesting, AReservedGapSurvivesGrowthAndShrink)
{
    KV::Foundation::Buffer buffer(64);
    buffer.ReservePrepend(8);
    AppendText(buffer, std::string(100000, 'x'));
    ASSERT_GT(buffer.Capacity(), 64u);

    buffer.ConsumeAll();
    buffer.Shrink();

    EXPECT_EQ(buffer.PrependableSize(), 8u);
    EXPECT_EQ(buffer.Capacity(), KV::Foundation::Buffer::kDefaultCapacity);
}

TEST(::Foundation::BufferTesting, ReservePrependNeverShrinksAnExistingGap)
{
    KV::Foundation::Buffer buffer(64);
    buffer.ReservePrepend(16);

    buffer.ReservePrepend(4);

    EXPECT_EQ(buffer.PrependableSize(), 16u);
}

TEST(::Foundation::BufferTesting, ReservePrependBeyondTheCapacityGrows)
{
    KV::Foundation::Buffer buffer(8);
    Receive(buffer, "12345678");

    buffer.ReservePrepend(64);

    EXPECT_EQ(buffer.PrependableSize(), 64u);
    EXPECT_EQ(buffer.StringView(), "12345678");
    EXPECT_GE(buffer.Capacity(), 72u);
}

// =============================================================================
// Consume
// =============================================================================

TEST(::Foundation::BufferTesting, PartialConsumeDoesNotMoveTheRemainingBytes)
{
    KV::Foundation::Buffer buffer(64);
    Receive(buffer, "0123456789");
    const char* before = buffer.Readable().data();

    buffer.Consume(4);

    // Consuming is a cursor move, not a memmove: the survivors stay put.
    EXPECT_EQ(buffer.Readable().data(), before + 4);
    EXPECT_EQ(buffer.StringView(), "456789");
}

TEST(::Foundation::BufferTesting, ConsumingOneRequestLeavesThePipelinedRemainder)
{
    KV::Foundation::Buffer buffer;
    Receive(buffer, "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n");

    buffer.Consume(std::string_view("*1\r\n$4\r\nPING\r\n").size());

    // The second pipelined request must survive; dropping it would desynchronise
    // the stream.
    EXPECT_EQ(buffer.StringView(), "*1\r\n$4\r\nPING\r\n");
}

TEST(::Foundation::BufferTesting, DrainingCompletelyRewindsBothCursors)
{
    KV::Foundation::Buffer buffer(32);
    Receive(buffer, "0123456789");

    buffer.Consume(10);

    EXPECT_TRUE(buffer.Empty());
    // A fully drained buffer must offer its whole capacity again without any
    // compaction, which is what keeps a steady stream allocation-free.
    EXPECT_EQ(buffer.WritableSize(), 32u);
    EXPECT_EQ(buffer.PrependableSize(), 0u);
}

TEST(::Foundation::BufferTesting, OverConsumingIsClampedRatherThanUndefined)
{
    KV::Foundation::Buffer buffer;
    Receive(buffer, "short");

    // A decoder that miscounts must not be able to walk the cursor past the data.
    buffer.Consume(9999);

    EXPECT_TRUE(buffer.Empty());
    EXPECT_EQ(buffer.ReadableSize(), 0u);
}

TEST(::Foundation::BufferTesting, ConsumingNothingChangesNothing)
{
    KV::Foundation::Buffer buffer;
    Receive(buffer, "payload");

    buffer.Consume(0);

    EXPECT_EQ(buffer.StringView(), "payload");
}

TEST(::Foundation::BufferTesting, ConsumeAllDropsEverythingIncludingPipelinedData)
{
    KV::Foundation::Buffer buffer;
    Receive(buffer, "first\r\nsecond\r\n");

    buffer.ConsumeAll();

    EXPECT_TRUE(buffer.Empty());
    EXPECT_EQ(buffer.StringView(), "");
}

// =============================================================================
// Compaction versus growth
// =============================================================================

TEST(::Foundation::BufferTesting, ReserveCompactsInsteadOfGrowingWhenTheFrontIsFree)
{
    KV::Foundation::Buffer buffer(16);
    const std::size_t capacity = buffer.Capacity();
    Receive(buffer, "0123456789");
    buffer.Consume(8); // 2 readable, 8 reclaimable at the front, 6 at the tail

    // The 6 tail bytes are not enough, but 8 + 6 = 14 is. Reclaiming the prefix
    // must be preferred over doubling.
    const std::span<char> room = buffer.Reserve(10);

    EXPECT_GE(room.size(), 10u);
    EXPECT_EQ(buffer.Capacity(), capacity);
    EXPECT_EQ(buffer.StringView(), "89"); // survivors slid to the front
}

TEST(::Foundation::BufferTesting, CompactionReclaimsThePrefixAndPreservesOrder)
{
    KV::Foundation::Buffer buffer(24);
    Receive(buffer, "HEADER:payload");
    buffer.Consume(7);
    ASSERT_EQ(buffer.StringView(), "payload");
    ASSERT_EQ(buffer.PrependableSize(), 7u);

    buffer.Reserve(20); // forces a compaction

    EXPECT_EQ(buffer.StringView(), "payload");
    // The prefix became writable room again.
    EXPECT_EQ(buffer.PrependableSize(), 0u);
}

TEST(::Foundation::BufferTesting, CompactionKeepsAReservedPrependGap)
{
    KV::Foundation::Buffer buffer(32);
    buffer.ReservePrepend(4);
    Receive(buffer, "HEADER:payload");
    buffer.Consume(7);
    ASSERT_EQ(buffer.PrependableSize(), 11u);

    buffer.Reserve(24); // forces a compaction

    // Compaction may only reclaim down to the reserved gap, never through it.
    EXPECT_EQ(buffer.StringView(), "payload");
    EXPECT_EQ(buffer.PrependableSize(), 4u);
}

TEST(::Foundation::BufferTesting, ReserveGrowsWhenCompactionCannotSatisfyTheRequest)
{
    KV::Foundation::Buffer buffer(16);
    Receive(buffer, "0123456789");
    buffer.Consume(2); // 8 readable, 2 reclaimable, 6 at the tail

    // 2 + 6 = 8 cannot cover 100, so the storage has to grow.
    buffer.Reserve(100);

    EXPECT_GE(buffer.Capacity(), 108u);
    EXPECT_EQ(buffer.StringView(), "23456789"); // and the live bytes survive the move
}

TEST(::Foundation::BufferTesting, GrowthIsGeometricAgainstWhatIsHeldNotTheCapacity)
{
    KV::Foundation::Buffer buffer(1024);
    Receive(buffer, "tiny");
    buffer.Consume(4); // drained: nothing live

    // The capacity is already 1024 and nothing is live, so asking for 16 must be
    // satisfied outright rather than doubling anything.
    buffer.Reserve(16);

    EXPECT_EQ(buffer.Capacity(), 1024u);
}

TEST(::Foundation::BufferTesting, AnExactFitDoesNotGrow)
{
    KV::Foundation::Buffer buffer(16);

    buffer.Reserve(16);

    EXPECT_EQ(buffer.Capacity(), 16u);
}

TEST(::Foundation::BufferTesting, ReserveBeyondTheCapacityKeepsDataThroughReallocation)
{
    KV::Foundation::Buffer buffer(8);
    Receive(buffer, "keepme");

    const std::string payload(4096, 'z');
    AppendText(buffer, payload);

    EXPECT_EQ(buffer.ReadableSize(), 6u + payload.size());
    EXPECT_EQ(buffer.StringView().substr(0, 6), "keepme");
    EXPECT_EQ(buffer.StringView().substr(6), payload);
}

// =============================================================================
// Streaming: the workloads this class exists for
// =============================================================================

TEST(::Foundation::BufferTesting, ARequestResponseStreamStaysOnOneAllocation)
{
    CountingResource resource;
    KV::Foundation::Buffer buffer(64, &resource);
    const std::size_t allocations = resource.GetAllocationCount();
    const std::size_t capacity = buffer.Capacity();

    // Five hundred complete request/consume cycles, each well inside the
    // capacity. Neither the capacity nor the allocation count may move: this is
    // the regression test for a buffer that grows or memmoves per request.
    for (int round = 0; round < 500; ++round)
    {
        Receive(buffer, "*1\r\n$4\r\nPING\r\n");
        ASSERT_EQ(buffer.StringView(), "*1\r\n$4\r\nPING\r\n") << "round " << round;
        buffer.Consume(buffer.ReadableSize());
        ASSERT_TRUE(buffer.Empty()) << "round " << round;
    }

    EXPECT_EQ(buffer.Capacity(), capacity);
    EXPECT_EQ(resource.GetAllocationCount(), allocations);
}

TEST(::Foundation::BufferTesting, AFramedResponseStreamStaysOnOneAllocation)
{
    CountingResource resource;
    KV::Foundation::Buffer buffer(64, &resource);
    buffer.ReservePrepend(8);
    const std::size_t allocations = resource.GetAllocationCount();

    // Append payload, prepend header, flush -- repeatedly. The reserved gap has
    // to keep every prepend off the reallocation path.
    for (int round = 0; round < 500; ++round)
    {
        AppendText(buffer, "payload");
        PrependText(buffer, "$7\r\n");
        ASSERT_EQ(buffer.StringView(), "$7\r\npayload") << "round " << round;
        buffer.ConsumeAll();
    }

    EXPECT_EQ(resource.GetAllocationCount(), allocations);
}

TEST(::Foundation::BufferTesting, APartiallyConsumedStreamStaysBounded)
{
    KV::Foundation::Buffer buffer(64);

    // Every round leaves a fragment behind, so the cursors never reset and the
    // buffer has to rely on compaction to stay bounded.
    for (int round = 0; round < 500; ++round)
    {
        Receive(buffer, "0123456789");
        buffer.Consume(10);
        Receive(buffer, "tail");
        buffer.Consume(2);
        buffer.Consume(2);
    }

    EXPECT_TRUE(buffer.Empty());
    EXPECT_LE(buffer.Capacity(), 64u);
}

TEST(::Foundation::BufferTesting, AFragmentedRequestIsReassembledByteByByte)
{
    KV::Foundation::Buffer buffer(8);
    const std::string_view request = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";

    // The pathological case: one byte per read.
    for (const char character : request)
    {
        Receive(buffer, std::string_view(&character, 1));
    }

    EXPECT_EQ(buffer.StringView(), request);
}

TEST(::Foundation::BufferTesting, InterleavedAppendAndConsumeBehavesLikeAWriteQueue)
{
    KV::Foundation::Buffer buffer(32);

    AppendText(buffer, "AAAA");
    buffer.Consume(2); // partial flush
    AppendText(buffer, "BBBB");
    buffer.Consume(2);
    AppendText(buffer, "CCCC");

    // Whatever was not flushed stays queued, in order, behind the new data.
    EXPECT_EQ(buffer.StringView(), "BBBBCCCC");
}

// =============================================================================
// Binary safety
// =============================================================================

TEST(::Foundation::BufferTesting, EmbeddedNullBytesSurvive)
{
    KV::Foundation::Buffer buffer;
    const std::string_view payload("a\0b\0c", 5);

    AppendText(buffer, payload);

    EXPECT_EQ(buffer.ReadableSize(), 5u);
    EXPECT_EQ(buffer.StringView(), payload);
}

TEST(::Foundation::BufferTesting, EmbeddedTerminatorsAreJustBytes)
{
    KV::Foundation::Buffer buffer;

    // A bulk string payload may contain the protocol's own delimiter; the buffer
    // must not attribute any meaning to it.
    AppendText(buffer, "$12\r\nline1\r\nline2\r\n");

    EXPECT_EQ(buffer.StringView(), "$12\r\nline1\r\nline2\r\n");
}

TEST(::Foundation::BufferTesting, ReadableMatchesTheStringView)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, "payload");
    buffer.Consume(2);

    const std::span<const char> readable = buffer.Readable();

    ASSERT_EQ(readable.size(), buffer.ReadableSize());
    EXPECT_EQ(std::memcmp(readable.data(), buffer.StringView().data(), readable.size()), 0);
}

TEST(::Foundation::BufferTesting, HighBitBytesAreNotSignExtendedAway)
{
    KV::Foundation::Buffer buffer;
    const std::string payload {'\x00', '\x7f', '\x80', '\xff'};

    AppendText(buffer, payload);

    ASSERT_EQ(buffer.ReadableSize(), 4u);
    EXPECT_EQ(static_cast<unsigned char>(buffer.StringView()[3]), 0xffu);
}

// =============================================================================
// Capacity management
// =============================================================================

TEST(::Foundation::BufferTesting, ClearDropsTheDataButKeepsTheAllocation)
{
    KV::Foundation::Buffer buffer(16);
    AppendText(buffer, std::string(4096, 'x'));
    const std::size_t capacity = buffer.Capacity();
    ASSERT_GT(capacity, 16u);

    buffer.Clear();

    EXPECT_TRUE(buffer.Empty());
    // Keeping the allocation is the point: the next connection to reuse this
    // buffer starts warm.
    EXPECT_EQ(buffer.Capacity(), capacity);
}

TEST(::Foundation::BufferTesting, ShrinkReleasesCapacityDownToTheDefault)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, std::string(100000, 'x'));
    buffer.ConsumeAll();
    ASSERT_GT(buffer.Capacity(), KV::Foundation::Buffer::kDefaultCapacity);

    buffer.Shrink();

    EXPECT_EQ(buffer.Capacity(), KV::Foundation::Buffer::kDefaultCapacity);
    EXPECT_TRUE(buffer.Empty());
}

TEST(::Foundation::BufferTesting, ShrinkKeepsTheLiveBytes)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, std::string(100000, 'x'));
    buffer.Consume(99993);
    ASSERT_EQ(buffer.ReadableSize(), 7u);

    buffer.Shrink();

    EXPECT_EQ(buffer.ReadableSize(), 7u);
    EXPECT_EQ(buffer.StringView(), "xxxxxxx");
    EXPECT_EQ(buffer.Capacity(), KV::Foundation::Buffer::kDefaultCapacity);
}

TEST(::Foundation::BufferTesting, ShrinkNeverGoesBelowWhatIsLive)
{
    KV::Foundation::Buffer buffer;
    const std::string payload(100000, 'x');
    AppendText(buffer, payload);

    buffer.Shrink();

    EXPECT_GE(buffer.Capacity(), payload.size());
    EXPECT_EQ(buffer.StringView(), payload);
}

TEST(::Foundation::BufferTesting, ShrinkOnASmall::Foundation::BufferIsHarmless)
{
    KV::Foundation::Buffer buffer(16);
    AppendText(buffer, "data");

    buffer.Shrink();

    EXPECT_EQ(buffer.StringView(), "data");
}

TEST(::Foundation::BufferTesting, AShrunk::Foundation::BufferIsStillUsable)
{
    KV::Foundation::Buffer buffer;
    AppendText(buffer, std::string(100000, 'x'));
    buffer.ConsumeAll();
    buffer.Shrink();

    Receive(buffer, "after shrink");

    EXPECT_EQ(buffer.StringView(), "after shrink");
}

// =============================================================================
// Move semantics
// =============================================================================

TEST(::Foundation::BufferTesting, MoveConstructionTransfersTheContent)
{
    KV::Foundation::Buffer source(64);
    Receive(source, "0123456789");
    source.Consume(4);

    const KV::Foundation::Buffer moved(std::move(source));

    EXPECT_EQ(moved.StringView(), "456789");
    EXPECT_EQ(moved.Capacity(), 64u);
}

TEST(::Foundation::BufferTesting, MoveAssignmentTransfersTheContent)
{
    KV::Foundation::Buffer source(64);
    Receive(source, "payload");
    KV::Foundation::Buffer target(16);
    Receive(target, "discarded");

    target = std::move(source);

    EXPECT_EQ(target.StringView(), "payload");
}

TEST(::Foundation::BufferTesting, AMovedFrom::Foundation::BufferIsStillUsable)
{
    KV::Foundation::Buffer source;
    Receive(source, "payload");
    const KV::Foundation::Buffer moved(std::move(source));

    // Moved-from is unspecified but valid: reusing it must not be undefined.
    source.Clear();
    AppendText(source, "reused");

    EXPECT_EQ(source.StringView(), "reused");
}

// =============================================================================
// Memory resource
// =============================================================================

TEST(::Foundation::BufferTesting, AllocatesFromTheSuppliedResource)
{
    CountingResource resource;

    {
        KV::Foundation::Buffer buffer(1024, &resource);
        EXPECT_EQ(buffer.GetMemoryResource(), &resource);
        EXPECT_GE(resource.GetAllocationCount(), 1u);
        EXPECT_GE(resource.GetLiveBytes(), 1024u);
    }

    EXPECT_EQ(resource.GetLiveBytes(), 0u); // and gives it all back
}

TEST(::Foundation::BufferTesting, GrowthKeepsUsingTheSuppliedResource)
{
    CountingResource resource;
    KV::Foundation::Buffer buffer(16, &resource);

    AppendText(buffer, std::string(100000, 'x'));

    EXPECT_EQ(buffer.GetMemoryResource(), &resource);
    EXPECT_GE(resource.GetLiveBytes(), 100000u);
}

TEST(::Foundation::BufferTesting, RebindingMovesTheStorageOntoTheNewResource)
{
    CountingResource first;
    CountingResource second;
    KV::Foundation::Buffer buffer(1024, &first);
    Receive(buffer, "payload");
    ASSERT_GT(first.GetLiveBytes(), 0u);

    buffer.UseMemoryResource(&second);

    EXPECT_EQ(buffer.GetMemoryResource(), &second);
    // The old resource must be released, not merely abandoned. Move *assignment*
    // would have failed this: a polymorphic_allocator does not propagate, so the
    // bytes would have stayed in first's block.
    EXPECT_EQ(first.GetLiveBytes(), 0u);
    EXPECT_GT(second.GetLiveBytes(), 0u);
}

TEST(::Foundation::BufferTesting, RebindingPreservesTheLiveBytes)
{
    CountingResource resource;
    KV::Foundation::Buffer buffer(64);
    Receive(buffer, "HEADER:payload");
    buffer.Consume(7);

    buffer.UseMemoryResource(&resource);

    // A rebind is a relocation, not a reset; silently losing buffered bytes
    // would desynchronise the connection.
    EXPECT_EQ(buffer.StringView(), "payload");
}

TEST(::Foundation::BufferTesting, RebindingPreservesAReservedPrependGap)
{
    CountingResource resource;
    KV::Foundation::Buffer buffer(64);
    buffer.ReservePrepend(8);
    AppendText(buffer, "payload");

    buffer.UseMemoryResource(&resource);

    EXPECT_EQ(buffer.PrependableSize(), 8u);
    EXPECT_EQ(buffer.StringView(), "payload");
}

TEST(::Foundation::BufferTesting, RebindingToTheSameResourceIsANoOp)
{
    CountingResource resource;
    KV::Foundation::Buffer buffer(64, &resource);
    Receive(buffer, "payload");
    const std::size_t allocations = resource.GetAllocationCount();

    buffer.UseMemoryResource(&resource);

    EXPECT_EQ(resource.GetAllocationCount(), allocations);
    EXPECT_EQ(buffer.StringView(), "payload");
}

TEST(::Foundation::BufferTesting, RebindingToNullIsIgnored)
{
    CountingResource resource;
    KV::Foundation::Buffer buffer(64, &resource);
    Receive(buffer, "payload");

    buffer.UseMemoryResource(nullptr);

    EXPECT_EQ(buffer.GetMemoryResource(), &resource);
    EXPECT_EQ(buffer.StringView(), "payload");
}

TEST(::Foundation::BufferTesting, ARebound::Foundation::BufferIsStillUsable)
{
    CountingResource resource;
    KV::Foundation::Buffer buffer(32);
    Receive(buffer, "before");

    buffer.UseMemoryResource(&resource);
    Receive(buffer, "-after");

    EXPECT_EQ(buffer.StringView(), "before-after");
    EXPECT_EQ(buffer.GetMemoryResource(), &resource);
}
