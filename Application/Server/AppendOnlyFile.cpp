#include "AppendOnlyFile.hpp"

#include <system_error>

namespace KV
{
namespace
{
RESP::Object Bulk(std::string value)
{
    return RESP::Object(RESP::BulkString{.value = std::move(value)});
}
} // namespace

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

    file_.open(path_, std::ios::binary | std::ios::app);
    enabled_ = file_.is_open();
    return enabled_;
}

void AppendOnlyFile::disable() noexcept
{
    if (file_.is_open())
    {
        file_.close();
    }
    enabled_ = false;
}

std::string_view AppendOnlyFile::command_name(CommandType type)
{
    switch (type)
    {
    case CommandType::kPing:
        return "PING";
    case CommandType::kGet:
        return "GET";
    case CommandType::kSet:
        return "SET";
    case CommandType::kDel:
        return "DEL";
    case CommandType::kExists:
        return "EXISTS";
    case CommandType::kExpire:
        return "EXPIRE";
    case CommandType::kTTL:
        return "TTL";
    case CommandType::kMulti:
        return "MULTI";
    case CommandType::kExec:
        return "EXEC";
    case CommandType::kAppendOnly:
        return "APPENDONLY";
    case CommandType::kSave:
        return "SAVE";
    }
    return "";
}

RESP::Object AppendOnlyFile::to_object(const Command &command)
{
    RESP::Array array;
    auto push = [&array](std::string value) {
        array.values.push_back(Bulk(std::move(value)));
    };

    push(std::string(command_name(command.type)));
    switch (command.type)
    {
    case CommandType::kPing:
    case CommandType::kMulti:
    case CommandType::kExec:
    case CommandType::kSave:
    case CommandType::kTTL:
        break;
    case CommandType::kGet:
        push(std::get<GetParams>(command.parameters).key);
        break;
    case CommandType::kSet: {
        const auto &set = std::get<SetParams>(command.parameters);
        push(set.key);
        push(set.value);
        break;
    }
    case CommandType::kDel:
        push(std::get<DelParams>(command.parameters).key);
        break;
    case CommandType::kExists:
        push(std::get<ExistsParams>(command.parameters).key);
        break;
    case CommandType::kExpire: {
        const auto &expire = std::get<ExpireParams>(command.parameters);
        push(expire.key);
        push(std::to_string(expire.ttl.count()));
        break;
    }
    case CommandType::kAppendOnly:
        push(std::get<AppendOnlyParams>(command.parameters).enabled ? "YES" : "NO");
        break;
    }

    return RESP::Object(std::move(array));
}

bool AppendOnlyFile::write_buffer(Foundation::Buffer &buffer)
{
    const auto bytes = buffer.string_view();
    if (!bytes.empty())
    {
        file_.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    const bool ok = file_.good();
    buffer.clear();
    return ok;
}

bool AppendOnlyFile::write_object(const RESP::Object &object)
{
    Foundation::Buffer buffer;
    auto encoder = RESP::Encode(object, buffer);
    while (encoder.poll() == RESP::EncodeStatus::kNeedFlush)
    {
        if (!write_buffer(buffer))
        {
            return false;
        }
    }
    if (!buffer.is_empty() && !write_buffer(buffer))
    {
        return false;
    }
    file_.flush();
    return file_.good();
}

bool AppendOnlyFile::append(const Command &command)
{
    if (!enabled_)
    {
        return true;
    }
    return write_object(to_object(command));
}
} // namespace KV