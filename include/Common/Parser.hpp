#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include "Common/Command.hpp"
#include "Common/Result.hpp"

namespace KV
{
// Outcome of a single protocol-resolution attempt against a byte stream.
enum class ProtocolStatus
{
    kOk, // a complete message was resolved from the input
    kIncomplete, // more bytes are required before a message can be resolved
    kProtocolError // the input violates the wire protocol
};

// Result of decoding a request (client -> server) from a byte stream.
struct RequestParse
{
    ProtocolStatus status = ProtocolStatus::kIncomplete;
    std::size_t consumed_bytes = 0;
    std::string error_message;
    Command command;
};

// Result of decoding a response (server -> client) from a byte stream.
struct ResponseParse
{
    ProtocolStatus status = ProtocolStatus::kIncomplete;
    std::string error_message;
    Result result;
};

// Networking-model-agnostic handler for the simplified RESP protocol.
//
// The parser sits on top of the networking model (Reactor / Proactor) and is
// the single place that turns commands and results into bytes and back again:
//   - Request : a Command encapsulated by the sender and decoded by the receiver.
//   - Response: a Result encapsulated by the sender and decoded by the receiver.
//
// It is intentionally stateless so a single instance can be shared freely, and
// so it can be embedded in a Session without affecting its move semantics.
class Parser
{
public:
    // Request encoding / decoding (client side encodes, server side decodes).
    std::string SerializeRequest(const Command& command) const;
    RequestParse ParseRequest(std::string_view input) const;

    // Response encoding / decoding (server side encodes, client side decodes).
    std::string SerializeResponse(const Result& result) const;
    ResponseParse ParseResponse(std::string_view input) const;
};
} // namespace KV
