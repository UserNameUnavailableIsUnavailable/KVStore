#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include "Protocol.hpp"

namespace KV
{

enum class ParseStatus
{
	kOk,
	kIncomplete,
	kProtocolError
};

struct ParseResult
{
	ParseStatus status = ParseStatus::kIncomplete;
	std::size_t consumed_bytes = 0;
	Request request;
	std::string error_message;
};

/*
* Parser decodes a single request frame from an input buffer.
*/
class Parser
{
public:
	ParseResult Parse(std::string_view input) const;
};

} // namespace KV