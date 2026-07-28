#include "Common/Parser.hpp"

#include <string>

namespace KV
{
namespace
{
ProtocolStatus MapCommandStatus(CommandParseStatus status)
{
    switch (status)
    {
        case CommandParseStatus::kOk:
            return ProtocolStatus::kOk;
        case CommandParseStatus::kIncomplete:
            return ProtocolStatus::kIncomplete;
        case CommandParseStatus::kProtocolError:
            return ProtocolStatus::kProtocolError;
    }
    return ProtocolStatus::kProtocolError;
}
} // namespace

std::string Parser::SerializeRequest(const Command& command) const
{
    return command.Serialize();
}

RequestParse Parser::ParseRequest(std::string_view input) const
{
    RequestParse parsed;
    const CommandParseResult result = parsed.command.Deserialize(input);
    parsed.status = MapCommandStatus(result.status);
    parsed.consumed_bytes = result.consumed_bytes;
    parsed.error_message = result.error_message;
    return parsed;
}

std::string Parser::SerializeResponse(const Result& result) const
{
    return result.Serialize();
}

ResponseParse Parser::ParseResponse(std::string_view input) const
{
    ResponseParse parsed;
    parsed.result.Deserialize(std::string(input));
    // Result::Deserialize does not distinguish "incomplete" from "malformed";
    // it only reports success/failure through the Result itself. Streaming /
    // partial response resolution (kIncomplete) is left as a future refinement.
    parsed.status = ProtocolStatus::kOk;
    return parsed;
}
} // namespace KV
