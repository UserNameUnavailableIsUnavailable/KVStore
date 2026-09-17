#include "WriteHistory.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <utility>

namespace KV
{
WriteHistory::WriteHistory(std::size_t capacity_bytes, std::size_t block_bytes) :
    capacity_(std::max<std::size_t>(capacity_bytes, 1)), block_bytes_(std::max<std::size_t>(block_bytes, 1))
{
}

WriteHistory::~WriteHistory() noexcept
{
    assert(cursors_.empty() && "a cursor outlived the log it was reading");
}

bool WriteHistory::recording() const noexcept
{
    // A cursor that has been told its place is gone is not reading anything any
    // more, so it is not a reason to keep encoding writes.
    return std::any_of(cursors_.begin(), cursors_.end(), [](const std::unique_ptr<Cursor> &cursor) {
        return cursor->valid_;
    });
}

void WriteHistory::append(const Command &command)
{
    if (!recording())
    {
        return;
    }

    const std::string encoded = EncodeCommand(command);

    // A new block is cut when the tail is full or there is no tail at all, so a
    // command larger than a block becomes a block of its own instead of being
    // split across two: a command is the unit a replica applies.
    if (blocks_.empty() || blocks_.back().bytes.size() >= block_bytes_)
    {
        Block block;
        block.offset = offset_;
        // Everything behind the write point is going to have to read it.
        for (const std::unique_ptr<Cursor> &cursor : cursors_)
        {
            if (cursor->valid_ && cursor->released_ < offset_)
            {
                ++block.pins;
            }
        }
        blocks_.push_back(std::move(block));
    }

    Block &tail = blocks_.back();
    const std::uint64_t before = tail.end();
    tail.bytes += encoded;
    const std::uint64_t after = tail.end();
    ++tail.commands;

    // The tail has grown, so a cursor that sits inside what was just added holds
    // this block now. The ones already below it were counted when it was cut,
    // and the ones above it do not hold it yet.
    for (const std::unique_ptr<Cursor> &cursor : cursors_)
    {
        if (cursor->valid_ && cursor->released_ >= before && cursor->released_ < after)
        {
            ++tail.pins;
        }
    }

    offset_ = after;
    bytes_ += encoded.size();
    ++commands_;

    release_head();
    trim();
}

WriteHistory::Cursor *WriteHistory::attach(std::uint64_t offset)
{
    if (offset > offset_ || offset < oldest_offset())
    {
        // The log holds neither a position ahead of what was ever written nor
        // one that has already left it. Either way this reader has to start from
        // a snapshot.
        return nullptr;
    }

    // Not makable by anyone else: the cursor belongs to the log that made it.
    auto cursor = std::unique_ptr<Cursor>(new Cursor(offset));
    for (Block &block : blocks_)
    {
        if (block.end() > offset)
        {
            ++block.pins;
        }
    }
    cursors_.push_back(std::move(cursor));
    return cursors_.back().get();
}

void WriteHistory::detach(Cursor *cursor) noexcept
{
    if (cursor == nullptr)
    {
        return;
    }

    release(*cursor, offset_);
    std::erase_if(cursors_, [cursor](const std::unique_ptr<Cursor> &held) {
        return held.get() == cursor;
    });
    release_head();
}

void WriteHistory::advance(Cursor &cursor, std::uint64_t offset)
{
    assert(cursor.valid_ && "an invalidated cursor cannot be caught up");
    assert(offset >= cursor.offset_ && "a cursor only moves forward");
    assert(offset <= offset_ && "a cursor cannot have read past the log");

    cursor.offset_ = offset;
    release(cursor, offset);
    release_head();
}

std::size_t WriteHistory::available(std::uint64_t offset, std::size_t limit) const
{
    std::size_t total = 0;
    for (const Block &block : blocks_)
    {
        if (block.end() <= offset || total == limit)
        {
            continue;
        }
        const std::uint64_t from = std::max(offset, block.offset);
        total += std::min<std::size_t>(static_cast<std::size_t>(block.end() - from), limit - total);
        offset = block.end();
    }
    return total;
}

std::size_t WriteHistory::copy(std::uint64_t offset, std::span<char> out) const
{
    std::size_t done = 0;
    for (const Block &block : blocks_)
    {
        if (block.end() <= offset || done == out.size())
        {
            continue;
        }
        const std::uint64_t from = std::max(offset, block.offset);
        const auto take = std::min<std::size_t>(static_cast<std::size_t>(block.end() - from), out.size() - done);
        std::memcpy(out.data() + done, block.bytes.data() + (from - block.offset), take);
        done += take;
        offset = block.end();
    }
    return done;
}

std::uint64_t WriteHistory::oldest_offset() const noexcept
{
    return blocks_.empty() ? offset_ : blocks_.front().offset;
}

void WriteHistory::release(Cursor &cursor, std::uint64_t offset)
{
    if (!cursor.valid_)
    {
        // An invalidated cursor was given back everything it held when it was
        // told, so there is nothing here that belongs to it.
        return;
    }

    for (Block &block : blocks_)
    {
        if (block.end() <= cursor.released_)
        {
            continue; // this cursor gave that block back already
        }
        if (block.end() > offset)
        {
            break;
        }
        assert(block.pins != 0 && "a cursor giving back a block it never held");
        --block.pins;
        cursor.released_ = block.end();
    }

    // A reader can be past bytes that have already left the log, so its position
    // moves with it whether or not there is a block left to give back.
    cursor.released_ = std::max(cursor.released_, offset);
}

void WriteHistory::invalidate(Cursor &cursor) noexcept
{
    for (Block &block : blocks_)
    {
        if (block.end() <= cursor.released_)
        {
            continue;
        }
        if (block.pins != 0)
        {
            --block.pins;
        }
        cursor.released_ = block.end();
    }

    // Whatever is written from here on is not held by a reader that has lost its
    // place: it is going to start from a snapshot.
    cursor.released_ = std::max(cursor.released_, offset_);
    cursor.valid_ = false;
}

void WriteHistory::erase_head()
{
    const Block &head = blocks_.front();
    bytes_ -= head.bytes.size();
    commands_ -= head.commands;
    blocks_.pop_front();
}

void WriteHistory::release_head()
{
    // The head leaves once no cursor holds it, which is why a reader that is
    // done never has to tell the log anything: giving back the last reference is
    // what moves the head.
    while (!blocks_.empty() && blocks_.front().pins == 0)
    {
        erase_head();
    }
}

void WriteHistory::trim()
{
    // The log has to stay bounded even when a replica is not reading it. The
    // head goes anyway once the capacity is passed, and the readers that still
    // needed it are marked rather than quietly handed a stream with a hole in
    // it: a replica that is told can start again, one that is not would go on
    // applying a stream with a gap in it.
    std::size_t lost_readers = 0;
    std::size_t lost_bytes = 0;

    while (bytes_ > capacity_ && blocks_.size() > 1)
    {
        const std::uint64_t end = blocks_.front().end();
        for (const std::unique_ptr<Cursor> &cursor : cursors_)
        {
            if (cursor->valid_ && cursor->released_ < end)
            {
                // A reader whose first unread block is leaving has lost its
                // place, so what it is losing is everything it had not read, not
                // just the block that happened to fall off the head.
                lost_bytes += static_cast<std::size_t>(offset_ - cursor->released_);
                invalidate(*cursor);
                ++lost_readers;
            }
        }

        erase_head();
        release_head();
    }

    if (lost_readers != 0)
    {
        dropped_ += lost_bytes;
        spdlog::warn("replication: the log dropped {} bytes that {} reader(s) had not reached; they have to synchronise again",
                     lost_bytes, lost_readers);
    }
}

bool WriteHistory::validate() const
{
    std::vector<std::size_t> expected(blocks_.size(), 0);
    for (const std::unique_ptr<Cursor> &cursor : cursors_)
    {
        if (!cursor->valid_)
        {
            continue;
        }
        for (std::size_t index = 0; index < blocks_.size(); ++index)
        {
            if (blocks_[index].end() > cursor->released_)
            {
                ++expected[index];
            }
        }
    }

    for (std::size_t index = 0; index < blocks_.size(); ++index)
    {
        if (blocks_[index].pins != expected[index])
        {
            return false;
        }
    }
    return true;
}
} // namespace KV
