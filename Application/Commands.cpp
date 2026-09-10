 #include "Commands.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <string_view>
#include <vector>

namespace KV
{
namespace
{
using Arguments = std::vector<std::string_view>;
using Validator = CommandValidation (*)(const Arguments &);

struct ValidatorEntry
{
	std::string_view name;
	Validator validator;
};

bool SameCommand(std::string_view left, std::string_view right);

CommandValidation Error(std::string message)
{
	return {.command = std::nullopt, .error = std::move(message)};
}

CommandValidation WrongArity(std::string_view command)
{
	return Error("ERR wrong number of arguments for '" + std::string(command) + "'");
}

template <typename Parameters> CommandValidation NoArguments(const Arguments &arguments, CommandType type, std::string_view name)
{
	if (arguments.size() != 1)
	{
		return WrongArity(name);
	}
	return {.command = Command{.type = type, .parameters = Parameters{}}, .error = {}};
}

CommandValidation ValidatePing(const Arguments &arguments)
{
	return NoArguments<PingParams>(arguments, CommandType::kPing, "PING");
}

CommandValidation ValidateGet(const Arguments &arguments)
{
	if (arguments.size() != 2)
	{
		return WrongArity("GET");
	}
	return {.command = Command{.type = CommandType::kGet, .parameters = GetParams{.key = std::string(arguments[1])}}, .error = {}};
}

CommandValidation ValidateSet(const Arguments &arguments)
{
	if (arguments.size() != 3)
	{
		return WrongArity("SET");
	}
	return {.command = Command{.type = CommandType::kSet,
							   .parameters = SetParams{.key = std::string(arguments[1]), .value = std::string(arguments[2])}},
			.error = {}};
}

CommandValidation ValidateDel(const Arguments &arguments)
{
	if (arguments.size() != 2)
	{
		return WrongArity("DEL");
	}
	return {.command = Command{.type = CommandType::kDel, .parameters = DelParams{.key = std::string(arguments[1])}}, .error = {}};
}

CommandValidation ValidateExists(const Arguments &arguments)
{
	if (arguments.size() != 2)
	{
		return WrongArity("EXISTS");
	}
	return {.command = Command{.type = CommandType::kExists, .parameters = ExistsParams{.key = std::string(arguments[1])}}, .error = {}};
}

CommandValidation ValidateMulti(const Arguments &arguments)
{
	return NoArguments<MultiParams>(arguments, CommandType::kMulti, "MULTI");
}

CommandValidation ValidateExec(const Arguments &arguments)
{
	return NoArguments<ExecParams>(arguments, CommandType::kExec, "EXEC");
}

CommandValidation ValidateAppendOnly(const Arguments &arguments)
{
	if (arguments.size() != 2)
	{
		return WrongArity("APPENDONLY");
	}
	if (SameCommand(arguments[1], "YES"))
	{
		return {.command = Command{.type = CommandType::kAppendOnly, .parameters = AppendOnlyParams{.enabled = true}}, .error = {}};
	}
	if (SameCommand(arguments[1], "NO"))
	{
		return {.command = Command{.type = CommandType::kAppendOnly, .parameters = AppendOnlyParams{.enabled = false}}, .error = {}};
	}
	return Error("APPENDONLY must be YES or NO");
}

CommandValidation ValidateSave(const Arguments &arguments)
{
	return NoArguments<SaveParams>(arguments, CommandType::kSave, "SAVE");
}

constexpr std::array<ValidatorEntry, 9> kValidators = {{{"PING", ValidatePing}, {"GET", ValidateGet}, {"SET", ValidateSet},
													   {"DEL", ValidateDel}, {"EXISTS", ValidateExists}, {"MULTI", ValidateMulti},
													   {"EXEC", ValidateExec}, {"APPENDONLY", ValidateAppendOnly}, {"SAVE", ValidateSave}}};

bool SameCommand(std::string_view left, std::string_view right)
{
	return left.size() == right.size() && std::ranges::equal(left, right, [](unsigned char a, unsigned char b) {
		return std::toupper(a) == std::toupper(b);
	});
}

std::optional<std::string_view> Text(const RESP::Object &object)
{
	if (const auto *simple = std::get_if<RESP::SimpleString>(&object.value))
	{
		return simple->value;
	}
	if (const auto *bulk = std::get_if<RESP::BulkString>(&object.value); bulk != nullptr && bulk->value)
	{
		return *bulk->value;
	}
	return std::nullopt;
}
} // namespace

CommandValidation ValidateCommand(const RESP::Object &request)
{
	const auto *array = std::get_if<RESP::Array>(&request.value);
	if (array == nullptr || array->values.empty())
	{
		return Error("ERR command must be a non-empty RESP array");
	}

	Arguments arguments;
	arguments.reserve(array->values.size());
	for (const RESP::Object &argument : array->values)
	{
		const auto text = Text(argument);
		if (!text)
		{
			return Error("ERR command arguments must be strings");
		}
		arguments.push_back(*text);
	}

	for (const ValidatorEntry &entry : kValidators)
	{
		if (SameCommand(arguments.front(), entry.name))
		{
			return entry.validator(arguments);
		}
	}
	return Error("ERR unknown command '" + std::string(arguments.front()) + "'");
}
} // namespace KV::Command
