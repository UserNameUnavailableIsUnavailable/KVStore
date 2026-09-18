#include "AppendOnlyFile.hpp"
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/NBIO.hpp>

#include <Foundation/Async/Async.hpp>

#include <system_error>
#include <iostream>

namespace KV
{
bool AppendOnlyFile::enable()
{
    if (enabled_)
    {
        return true;
    }

    if (const auto parent = path_.parent_path(); !parent.empty())
    {
        std::error_code error;
        std::filesystem::create_directories(parent, error);
        if (error)
        {
            return false;
        }
    }

    file_ = Foundation::NBIO::open_file(path_);
    enabled_ = static_cast<bool>(file_);
    return enabled_;
}

void AppendOnlyFile::disable() noexcept
{
    file_.reset();
    enabled_ = false;
}

// The entry an append writes is the unit the log is made of, so it is encoded in
// full before any of it is written and handed to the file as one write. The
// channel queues the writes behind each other, which is what makes that work: two
// entries that interleaved mid-command would leave a log that cannot be replayed.
//
// The buffer therefore grows rather than being drained as it fills. The bound is
// what the receive buffer can hold -- a command cannot be larger than the buffer
// that decoded it -- plus room for the encoding around it. An entry past that is
// not a command this server received, and writing part of it would be worse than
// refusing it.
constexpr std::size_t kInitialEntryBytes = 1024;
constexpr std::size_t kMaximumEntryBytes = (16U * 1024U * 1024U) + (64U * 1024U);

Foundation::NBIO::Task<void> AppendOnlyFile::append(const Command &command)
{
    if (!enabled_)
    {
        co_return;
    }

    // The append holds a reference of its own to the file: CONFIG APPENDONLY NO
    // resets the member, and an append that is already running has to finish the
    // entry it started rather than read a file that is no longer there.
    const std::shared_ptr<Foundation::NBIO::FileStream> file = file_;

    try
    {
        Foundation::Core::Buffer buffer{kInitialEntryBytes, kMaximumEntryBytes};
        const auto object = CommandToRESP(command);
        auto encoder = RESP::Encode(object, buffer);
        while (encoder.poll() == RESP::EncodeStatus::kNeedFlush)
        {
            // The encoder wants room, not a flush: growing the buffer keeps the
            // entry in one piece and the write that follows to one call.
            if (!buffer.reserve(buffer.capacity()))
            {
                throw std::runtime_error("AOF entry is larger than the log can hold");
            }
        }

        const std::size_t size = buffer.readable_size();
        const auto result = co_await file->write(buffer.readable_span());
        if (!result || *result != size)
        {
            throw std::runtime_error("failed to append AOF entry");
        }
    }
    catch (const std::exception &ex)
    {
        std::cerr << "AOF append failed: " << ex.what() << '\n';
        throw;
    }
    catch (...)
    {
        std::cerr << "AOF append failed: unknown exception\n";
        throw;
    }
    co_return;
}
} // namespace KV
