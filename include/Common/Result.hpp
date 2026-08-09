#pragma once

#include <string>
#include <vector>

namespace KV
{
enum class ResultType
{
	kSimpleString,
	kError,
	kBulkString,
	kArray,
};

struct Result
{
	ResultType type = ResultType::kSimpleString;
	std::pmr::string value;
	std::pmr::vector<Result> elements;
};
} // namespace KV
