#pragma once

#include <Application/Commands.hpp>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace KV
{
// The writes a master has applied, in the order it applied them, encoded once as
// the RESP bytes they go out as: the same bytes every replica reads, which is
// the point of keeping the log in this shape rather than sending each replica
// its own.
class WriteHistory
{
  public:
    // How much of the log may be held before the slowest replica starts losing
    // it. This is a decision about how long a link may stay down, not about
    // memory: it has to cover a snapshot transfer at the write rate of the
    // master, so it follows the size of the store and the speed of the link.
    static constexpr std::size_t kDefaultCapacityBytes = 4U << 20U;

    // Blocks are cut at this size, so what a block holds never moves once it is
    // closed and its reference count is settled as it is written. A command
    // larger than this gets a block of its own rather than being split, because
    // a command is the unit a replica applies.
    static constexpr std::size_t kDefaultBlockBytes = 64U << 10U;

    // A reader's position in the log. The history owns the cursor and hands out
    // a borrowed pointer that dies at Detach; the reader keeps that pointer
    // across an await, which is the whole reason the reference count exists.
    class Cursor
    {
      public:
        Cursor(const Cursor &) = delete;
        Cursor &operator=(const Cursor &) = delete;

        // The offset of the next byte this reader has not read.
        std::uint64_t offset() const noexcept
        {
            return offset_;
        }

        // False once the log has dropped bytes this cursor had not read. The
        // reader cannot be caught up from here any more.
        bool valid() const noexcept
        {
            return valid_;
        }

      private:
        friend class WriteHistory;

        explicit Cursor(std::uint64_t offset) noexcept : offset_(offset), released_(offset)
        {
        }

        std::uint64_t offset_{0};
        // Blocks whose end is at or below this have already been given back by
        // this cursor, which is what stops one advance from giving a block back
        // twice when the reader has moved past bytes that were never written.
        std::uint64_t released_{0};
        bool valid_{true};
    };

    explicit WriteHistory(std::size_t capacity_bytes = kDefaultCapacityBytes, std::size_t block_bytes = kDefaultBlockBytes);

    // Every cursor has to be detached first: the history owns them, so one that
    // outlived the log would be pointing at freed blocks.
    ~WriteHistory() noexcept;

    WriteHistory(const WriteHistory &) = delete;
    WriteHistory &operator=(const WriteHistory &) = delete;

    // Appends a write. It is encoded here, once, in the order the master applied
    // it, and the next offset moves with it -- but only while a cursor is there
    // to read it: a log nobody is following would be memory spent on nobody.
    void append(const Command &command);

    // A cursor at `offset`: the end of the log when a replica is handed a
    // snapshot, or where it had got to when it asks for the rest. Nothing when
    // the log no longer holds that offset, which is how a replica learns it has
    // to synchronise from scratch.
    Cursor *attach(std::uint64_t offset);
    void detach(Cursor *cursor) noexcept;

    // Acknowledges that everything below `offset` has been applied, which is
    // what gives those blocks back. Only forward, and never past the end of what
    // was written.
    void advance(Cursor &cursor, std::uint64_t offset);

    // What a reader at `offset` may take, up to `limit`, and the copying itself.
    // The reader holds a cursor at that offset, which is what keeps these bytes
    // from being dropped while it sends them.
    std::size_t available(std::uint64_t offset, std::size_t limit) const;
    std::size_t copy(std::uint64_t offset, std::span<char> out) const;

    // True while at least one replica can still be caught up, which is when the
    // server has to record what it applies.
    bool recording() const noexcept;

    // Where the next write goes. Monotonic for the life of the server.
    std::uint64_t end_offset() const noexcept
    {
        return offset_;
    }

    // The first byte still held, which is `end_offset()` when nothing is held.
    std::uint64_t oldest_offset() const noexcept;

    std::size_t bytes() const noexcept
    {
        return bytes_;
    }

    std::size_t commands() const noexcept
    {
        return commands_;
    }

    // What the readers have lost: bytes that were still unread when the log had
    // to let them go. Every reader counted here is a replica that could not be
    // caught up and has to start again from a snapshot.
    std::size_t dropped_bytes() const noexcept
    {
        return dropped_;
    }

    std::size_t cursors() const noexcept
    {
        return cursors_.size();
    }

    // Recomputes every reference count from the cursors and answers whether the
    // maintained ones agree. The counts are the one piece of state that can
    // drift without anyone noticing -- too high and the log never shrinks, too
    // low and a block is dropped while a reader is still owed it -- so this is
    // what the tests assert on rather than a comment.
    bool validate() const;

  private:
    struct Block
    {
        std::uint64_t offset{0};
        std::string bytes;
        std::size_t commands{0};
        std::size_t pins{0};

        std::uint64_t end() const noexcept
        {
            return offset + bytes.size();
        }
    };

    // Gives back the blocks this cursor has read up to `offset`, which is the
    // only place a reference count goes down along a stream.
    void release(Cursor &cursor, std::uint64_t offset);
    // Gives back everything the cursor still holds, including the blocks the
    // other readers need, and marks it as one that has to start again.
    void invalidate(Cursor &cursor) noexcept;
    // Drops the head while nobody holds it.
    void release_head();
    // Keeps the log within the capacity, dropping the head of readers that never
    // reached it.
    void trim();
    void erase_head();

    std::deque<Block> blocks_;
    std::vector<std::unique_ptr<Cursor>> cursors_;
    std::size_t capacity_{kDefaultCapacityBytes};
    std::size_t block_bytes_{kDefaultBlockBytes};
    std::size_t bytes_{0};
    std::size_t commands_{0};
    std::size_t dropped_{0};
    std::uint64_t offset_{0};
};
} // namespace KV
