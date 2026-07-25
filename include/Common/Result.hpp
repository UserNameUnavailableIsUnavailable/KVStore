#pragma once

#include <string>

namespace KV
{
class Result
{
public:
    Result() = default;
    Result(bool ok, std::string message, std::string result);

	std::string Serialize() const;
	void Deserialize(const std::string& serialized);
	bool Ok() const { return ok_; }
	const std::string& GetMessage() const { return message_; }
	const std::string& GetResult() const { return result_; }
private:
	bool ok_ = false;
	std::string message_;
	std::string result_;
};

std::string SerializeResult(bool ok, const std::string& message, const std::string& result);
} // KV
