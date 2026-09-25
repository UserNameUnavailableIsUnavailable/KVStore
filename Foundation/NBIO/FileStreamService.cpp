#include "FileStreamService.hpp"

#include <Foundation/NBIO/Engine.hpp>

#include <span>
#include <utility>

namespace Foundation::NBIO
{
FileStreamService::FileStreamService(const std::filesystem::path &path, Foundation::Core::FileMode mode,
                                     ::mode_t permissions) :
    // `Open` is what the file's two channels are built from, and it is the engine of
    // this thread that will watch them.
    stream_(FileStream::Open(path.string(), mode, permissions, Engine::multiplexer(), Engine::scheduler()))
{
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> FileStreamService::read(std::span<char> buffer)
{
    return stream_->read(buffer);
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> FileStreamService::write(std::span<const char> buffer)
{
    return stream_->write(buffer);
}
} // namespace Foundation::NBIO
