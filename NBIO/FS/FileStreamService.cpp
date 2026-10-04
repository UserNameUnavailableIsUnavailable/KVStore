#include "FileStreamService.hpp"

#include <NBIO/Runtime/Runtime.hpp>
#include <span>
#include <utility>

namespace NBIO::FS {
FileStreamService::FileStreamService(const std::filesystem::path& path, NBIO::FS::FileMode mode,
                                     std::filesystem::perms permissions)
    :  // `FileStream` is what the two channels are built on, and it is the engine of this
       // thread that will watch them.
    stream_(path.string(), mode, permissions, NBIO::Runtime::multiplexer(), NBIO::Runtime::scheduler()) {}

NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> FileStreamService::read(std::span<char> buffer) {
    return stream_.read(buffer);
}

NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> FileStreamService::write(
    std::span<const char> buffer) {
    return stream_.write(buffer);
}
}  // namespace NBIO::FS




