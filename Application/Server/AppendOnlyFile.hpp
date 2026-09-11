#pragma once

#include <Application/Commands.hpp>

#include <Foundation/Buffer.hpp>
#include <Application/RESP/RESP.hpp>
#include <Foundation/Async/FileStream.hpp>
#include <Foundation/Async/Task.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>

namespace KV
{
class AppendOnlyFile
{
  public:
    explicit AppendOnlyFile(std::filesystem::path path = "appendonly.aof") : path_(std::move(path))
    {
    }

    bool enable();
    void disable() noexcept;
    bool enabled() const noexcept
    {
        return enabled_;
    }

    Foundation::Async::Task<void> append(const Command &command);

    template <typename Apply> bool replay(Apply &&apply) const
    {
        if (!std::filesystem::exists(path_))
        {
            return true;
        }

        std::ifstream file(path_, std::ios::binary);
        if (!file)
        {
            return false;
        }

        const std::string bytes{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
        Foundation::Buffer buffer(std::max<std::size_t>(512, bytes.size()), std::max<std::size_t>(512, bytes.size()));
        if (!bytes.empty() && !buffer.append(bytes.data(), bytes.size()))
        {
            return false;
        }

        while (!buffer.is_empty())
        {
            auto decoder = RESP::Decode(buffer);
            while (!decoder.done())
            {
                decoder.resume();
            }
            if (decoder.status() != RESP::DecodeStatus::kComplete || !decoder.result().object.has_value())
            {
                return false;
            }

            const auto validation = ValidateCommand(*decoder.result().object);
            if (!validation || !validation.command.has_value())
            {
                return false;
            }
            if (!apply(*validation.command))
            {
                return false;
            }
        }
        return true;
    }

  private:
    static RESP::Object to_object(const Command &command);
    static std::string_view command_name(CommandType type);

    std::filesystem::path path_;
        std::shared_ptr<Foundation::Async::FileStream> file_;
    bool enabled_{false};
};
} // namespace KV