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

Foundation::NBIO::Task<void> AppendOnlyFile::append(const Command &command)
{
    if (!enabled_)
    {
        co_return;
    }

    try
    {
        Foundation::Core::Buffer buffer;
        const auto object = CommandToRESP(command);
        auto encoder = RESP::Encode(object, buffer);
        while (encoder.poll() == RESP::EncodeStatus::kNeedFlush)
        {
            auto result = co_await file_->write(buffer.readable_span());
            if (!result)
            {
                throw std::runtime_error("failed to append AOF entry");
            }
            buffer.consume(*result);
        }
        if (!buffer.is_empty())
        {
            auto result = co_await file_->write(buffer.readable_span());
            if (!result)
            {
                throw std::runtime_error("failed to append AOF entry");
            }
            buffer.consume(*result);
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
