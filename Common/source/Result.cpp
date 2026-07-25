#include "Common/Result.hpp"

#include <charconv>
#include <string>
#include <string_view>

namespace KV
{
/// RESP-like (de)serialization
/// +OK\r\n | -ERR\r\n
/// $<length>\r\n<data>\r\n

namespace
{
bool ParseLength(std::string_view length_text, std::size_t& length)
{
	if (length_text.empty())
	{
		return false;
	}

	const char* begin = length_text.data();
	const char* end = begin + length_text.size();
	const auto [parsed_end, error] = std::from_chars(begin, end, length);
	return error == std::errc {} && parsed_end == end;
}
} // namespace

Result::Result(bool ok, std::string message, std::string result) :
	ok_(ok),
	message_(std::move(message)),
	result_(std::move(result))
{
}

std::string Result::Serialize() const
{
	std::string status = ok_ ? "+OK" : "-ERR";
	if (!message_.empty())
	{
		status += ' ';
		status += message_;
	}
	return status + "\r\n$" +
		std::to_string(result_.size()) + "\r\n" + result_ + "\r\n";
}

void Result::Deserialize(const std::string& serialized)
{
	const std::string_view input(serialized);
	const std::size_t status_end = input.find("\r\n");
	if (status_end == std::string_view::npos || status_end == 0)
	{
		ok_ = false;
		message_ = "protocol error";
		result_.clear();
		return;
	}

	const bool success = input.substr(0, 3) == "+OK";
	const bool failure = input.substr(0, 4) == "-ERR";
	const std::size_t prefix_length = success ? 3 : 4;
	if ((!success && !failure) ||
		(status_end > prefix_length && input[prefix_length] != ' '))
	{
		ok_ = false;
		message_ = "protocol error";
		result_.clear();
		return;
	}

	const std::size_t bulk_marker = status_end + 2;
	if (bulk_marker >= input.size() || input[bulk_marker] != '$')
	{
		ok_ = false;
		message_ = "protocol error";
		result_.clear();
		return;
	}

	const std::size_t length_start = bulk_marker + 1;
	const std::size_t length_end = input.find("\r\n", length_start);
	std::size_t result_length = 0;
	if (length_end == std::string_view::npos ||
		!ParseLength(input.substr(length_start, length_end - length_start), result_length))
	{
		ok_ = false;
		message_ = "protocol error";
		result_.clear();
		return;
	}

	const std::size_t result_start = length_end + 2;
	if (result_length > input.size() - result_start ||
		result_start + result_length + 2 != input.size() ||
		input.substr(result_start + result_length) != "\r\n")
	{
		ok_ = false;
		message_ = "protocol error";
		result_.clear();
		return;
	}

	ok_ = success;
	const std::size_t message_start = status_end == prefix_length ? status_end : prefix_length + 1;
	message_ = std::string(input.substr(message_start, status_end - message_start));
	result_ = std::string(input.substr(result_start, result_length));
}

std::string SerializeResult(bool ok, const std::string& message, const std::string& result)
{
	return Result(ok, message, result).Serialize();

}
} // KV
