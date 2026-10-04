#pragma once

#include <CRC.h>

#include <Application/Commands.hpp>
#include <Application/RESP/RESP.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/NBIO/FileStreamService.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>

namespace KV {
class AppendOnlyFile {
   public:
    explicit AppendOnlyFile(std::filesystem::path path = "appendonly.aof") : path_(std::move(path)) {}

    bool enable();
    void disable() noexcept;
    bool enabled() const noexcept { return enabled_; }

    // Whether there is a log to read. Asked of the file rather than of enabled():
    // whether this instance has just turned the log on says nothing about whether
    // an earlier run left one behind, and which of the two files is the store when
    // the server starts is a question about the files.
    bool exists() const noexcept {
        std::error_code error;
        return std::filesystem::exists(path_, error);
    }

    const std::filesystem::path& path() const noexcept { return path_; }

    // Whether every entry written from here on ends with a checksum of itself. The
    // log's shape is the same either way: a checksum is an extra object after the
    // command, so a reader tells the difference by what it finds.
    void checksum(bool enabled) noexcept { checksum_ = enabled; }

    bool checksum() const noexcept { return checksum_; }

    Foundation::NBIO::Task<void> append(const Command& command);

    template <typename Apply>
    bool replay(Apply&& apply) const {
        if (!std::filesystem::exists(path_)) {
            return true;
        }

        std::ifstream file(path_, std::ios::binary);
        if (!file) {
            return false;
        }

        const std::string bytes{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
        Foundation::Core::Buffer buffer(std::max<std::size_t>(512, bytes.size()),
                                        std::max<std::size_t>(512, bytes.size()));
        if (!bytes.empty() && !buffer.write(bytes.data(), bytes.size())) {
            return false;
        }

        while (!buffer.is_empty()) {
            // The bytes of the command as the log holds them, kept before the decoder
            // takes them: the checksum that may follow is a checksum of exactly these.
            // Re-encoding what the command decodes to would only agree if nothing had
            // ever written an entry in another shape, and a log is read for what it
            // says rather than for what this program would have said.
            const std::span<const char> remaining = buffer.readable_span();

            auto decoder = RESP::Decode(buffer);
            while (!decoder.done()) {
                decoder.resume();
            }
            if (decoder.status() != RESP::DecodeStatus::kComplete || !decoder.result().object.has_value()) {
                return false;
            }

            const auto validation = ValidateCommand(*decoder.result().object);
            if (!validation || !validation.command.has_value()) {
                return false;
            }

            const std::size_t consumed = remaining.size() - buffer.readable_span().size();
            if (!verify_checksum(buffer, remaining.first(consumed))) {
                return false;
            }

            if (!apply(*validation.command)) {
                return false;
            }
        }
        return true;
    }

   private:
    // Answers whether what follows a command is a checksum of it, consuming the
    // checksum when it is one. A log holds a command after every command and a
    // checksum after only some of them, so this reads what is there rather than what
    // the setting would have written: a log checksummed for part of its life, and one
    // written before there were checksums at all, both replay.
    static bool verify_checksum(Foundation::Core::Buffer& buffer, std::span<const char> command);

    std::filesystem::path path_;
    // The file and the channels that write it, made against this thread's engine. Held
    // by the shared_ptr rather than by value because an append in flight keeps a
    // reference of its own and has to outlive a disable() that resets this.
    std::shared_ptr<Foundation::NBIO::FileStreamService> file_;
    bool enabled_{false};
    bool checksum_{false};
};
}  // namespace KV
