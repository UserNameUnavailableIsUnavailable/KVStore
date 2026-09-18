 #include "Commands.hpp"

#include <Foundation/Core/Address.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <string_view>
#include <vector>

namespace KV
{
namespace
{
// The words of the command, however they were obtained: decoded out of a request
// into strings, or read where they lie in the receive buffer as views. Everything
// below asks the same questions of them, so a command is answered the same way
// whichever of the two paths read it.
using Arguments = std::span<const std::string_view>;
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

CommandValidation ValidateInfo(const Arguments &arguments)
{
	return NoArguments<InfoParams>(arguments, CommandType::kInfo, "INFO");
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

// `DBSIZE` takes nothing and answers with a count, so there is nothing to
// validate beyond the command standing alone.
CommandValidation ValidateDbSize(const Arguments &arguments)
{
	return NoArguments<DbSizeParams>(arguments, CommandType::kDbSize, "DBSIZE");
}

CommandValidation ValidateMulti(const Arguments &arguments)
{
	return NoArguments<MultiParams>(arguments, CommandType::kMulti, "MULTI");
}

CommandValidation ValidateExec(const Arguments &arguments)
{
	return NoArguments<ExecParams>(arguments, CommandType::kExec, "EXEC");
}

// CONFIG names and values are case-insensitive in Redis, so normalise the parts
// that are compared as text.
std::string Lowercase(std::string_view text)
{
	std::string lowered{ text };
	std::ranges::transform(lowered, lowered.begin(), [](unsigned char character) {
		return static_cast<char>(std::tolower(character));
	});
	return lowered;
}

bool KnownConfigParameter(std::string_view name)
{
	return name == "appendonly" || name == "appendfsync" || name == "save" || name == "port" || name == "replication_address";
}

// A port as a parameter value. The lowest one is the caller's: `port` is a port
// to listen on, where 0 would mean "anything free", and `replication_address` takes
// the replication port, where 0 is how a server says it serves no replicas.
bool IsPort(std::string_view text, unsigned long lowest)
{
	try
	{
		std::size_t consumed = 0;
		const unsigned long port = std::stoul(std::string{ text }, &consumed);
		return consumed == text.size() && port >= lowest && port <= 65535;
	}
	catch (const std::exception &)
	{
		return false;
	}
}

// An address to serve replicas from: one the listener can bind, and one that
// names a device. A wildcard is accepted by rdma_bind_addr and names no device
// at all, so it is refused while the message can still say why.
bool IsDeviceAddress(std::string_view text)
{
	if (text == "0.0.0.0" || text == "::")
	{
		return false;
	}
	try
	{
		(void)Foundation::Core::Address::from_ipv4(text, 1);
	}
	catch (const std::exception &)
	{
		return false;
	}
	return true;
}

CommandValidation ConfigSubcommandError(std::string_view subcommand)
{
	return Error("ERR Unknown subcommand or wrong number of arguments for '" + std::string(subcommand) +
				 "'. Try CONFIG HELP.");
}

// `COMMAND` and its subcommands (`DOCS`, `COUNT`, `INFO`) describe the command
// table. redis-cli sends `COMMAND DOCS` as soon as it connects, so answering it
// is what keeps that probe from surfacing as "unknown command 'COMMAND'". There
// is no metadata to publish here, so every form answers with an empty array.
CommandValidation ValidateCommandInfo(const Arguments &arguments)
{
	(void)arguments;
	return {.command = Command{.type = CommandType::kCommand, .parameters = CommandParams{}}, .error = {}};
}

// The write form of CONFIG, whichever spelling carried it: one place decides what
// a parameter takes, and what the message is when it is given something else.
CommandValidation ValidateConfigWrite(const Arguments &arguments, std::size_t parameter_index)
{
	ConfigParams parameters;
	parameters.parameter = Lowercase(arguments[parameter_index]);
	for (std::size_t index = parameter_index + 1; index < arguments.size(); ++index)
	{
		parameters.values.emplace_back(Lowercase(arguments[index]));
	}

	if (!KnownConfigParameter(parameters.parameter))
	{
		return Error("ERR Unknown option or number of arguments for CONFIG SET - '" + parameters.parameter + "'");
	}
	if (parameters.parameter == "appendonly")
	{
		if (parameters.values.size() != 1 || (parameters.values.front() != "yes" && parameters.values.front() != "no"))
		{
			return Error("ERR CONFIG SET failed (possibly related to argument 'appendonly') - argument must be 'yes' or 'no'");
		}
	}
	else if (parameters.parameter == "port")
	{
		if (parameters.values.size() != 1 || !IsPort(parameters.values.front(), 1))
		{
			return Error("ERR CONFIG SET failed - 'port' wants a port between 1 and 65535");
		}
	}
	else if (parameters.parameter == "replication_address")
	{
		if (parameters.values.size() != 2 || !IsDeviceAddress(parameters.values.front()) ||
			!IsPort(parameters.values.back(), 0))
		{
			return Error("ERR CONFIG SET failed - 'replication_address' wants <ip> <port>, the address of the RDMA device to "
						  "serve replicas from");
		}
	}
	else
	{
		// This server never fsyncs and takes no automatic snapshots, so a value it
		// cannot honour is refused rather than silently accepted.
		return Error("ERR CONFIG SET failed - '" + parameters.parameter + "' is read-only on this server");
	}
	return { .command = Command{ .type = CommandType::kConfig, .parameters = std::move(parameters) }, .error = { } };
}

// `CONFIG GET <parameter>` and `CONFIG SET <parameter> <value>`, spelled the way
// Redis spells them so redis-cli and redis-benchmark can drive this server.
CommandValidation ValidateConfig(const Arguments &arguments)
{
	if (arguments.size() == 1)
	{
		return WrongArity("CONFIG");
	}

	const bool reading = SameCommand(arguments[1], "GET");
	const bool writing = SameCommand(arguments[1], "SET");
	if (!reading && !writing)
	{
		// `CONFIG <parameter> <value>...`: the write form with the word left out,
		// which is what a command file wants to write, one setting per line. No
		// parameter is named like a subcommand, so nothing else can mean this.
		if (arguments.size() < 3)
		{
			return ConfigSubcommandError(arguments[1]);
		}
		return ValidateConfigWrite(arguments, 1);
	}
	if (reading && arguments.size() != 3)
	{
		return ConfigSubcommandError(arguments[1]);
	}
	if (writing && arguments.size() < 4)
	{
		return ConfigSubcommandError(arguments[1]);
	}

	if (reading)
	{
		// An unknown name is not an error for the read form: Redis answers it with
		// an empty array, and only the server knows which names it has.
		ConfigParams parameters;
		parameters.parameter = Lowercase(arguments[2]);
		return {.command = Command{.type = CommandType::kConfig, .parameters = std::move(parameters)}, .error = {}};
	}
	return ValidateConfigWrite(arguments, 2);
}

// `CLIENT` and its subcommands. The handshake ones have to succeed -- a client
// announces itself with `CLIENT SETINFO` the moment it connects, and an error
// there makes it treat the connection as broken -- so the ones that only
// describe or name the connection answer `OK` and nothing is stored.
CommandValidation ValidateClient(const Arguments &arguments)
{
	if (arguments.size() < 2)
	{
		return WrongArity("CLIENT");
	}

	const std::string subcommand = Lowercase(arguments[1]);
	const auto arity = [&arguments, &subcommand](std::size_t expected) -> std::optional<CommandValidation> {
		if (arguments.size() == expected)
		{
			return std::nullopt;
		}
		return Error("ERR Unknown subcommand or wrong number of arguments for '" + subcommand + "'. Try CLIENT HELP.");
	};

	// Each entry is the subcommand and the argument count it takes.
	std::size_t expected = 0;
	if (subcommand == "setinfo")
	{
		expected = 4;
	}
	else if (subcommand == "setname")
	{
		expected = 3;
	}
	else if (subcommand == "getname" || subcommand == "id" || subcommand == "info" || subcommand == "list")
	{
		expected = 2;
	}
	else if (subcommand == "no-evict" || subcommand == "no-touch")
	{
		expected = 3;
	}
	else
	{
		return Error("ERR Unknown subcommand or wrong number of arguments for '" + subcommand + "'. Try CLIENT HELP.");
	}

	if (const auto wrong = arity(expected))
	{
		return *wrong;
	}

	ClientParams parameters;
	parameters.subcommand = subcommand;
	for (std::size_t index = 2; index < arguments.size(); ++index)
	{
		parameters.arguments.emplace_back(arguments[index]);
	}
	return {.command = Command{.type = CommandType::kClient, .parameters = std::move(parameters)}, .error = {}};
}

// `SAVE` forks a child to write the snapshot, which is what Redis calls
// `BGSAVE`, so that is the name this server answers to.
CommandValidation ValidateBgSave(const Arguments &arguments)
{
	return NoArguments<BgSaveParams>(arguments, CommandType::kBgSave, "BGSAVE");
}

constexpr std::array<ValidatorEntry, 13> kValidators = {{{ "PING", ValidatePing }, {"GET", ValidateGet}, {"SET", ValidateSet},
										   {"DEL", ValidateDel}, {"EXISTS", ValidateExists}, {"MULTI", ValidateMulti},
										   {"EXEC", ValidateExec}, {"COMMAND", ValidateCommandInfo}, {"CLIENT", ValidateClient},
										   {"DBSIZE", ValidateDbSize}, {"INFO", ValidateInfo},
										   {"CONFIG", ValidateConfig}, {"BGSAVE", ValidateBgSave}}};

// The case of one ASCII letter, without the C library. `std::toupper` is a call
// through the locale for every character, and this comparison runs for every
// character of every command a client sends -- a hundred times a microsecond at
// the rates this server answers at, where the profile put it at 3-4% on its own.
// The protocol is ASCII, so folding the case is an add.
constexpr char FoldCase(char character) noexcept
{
	return character >= 'a' && character <= 'z' ? static_cast<char>(character - ('a' - 'A')) : character;
}

bool SameCommand(std::string_view left, std::string_view right)
{
	if (left.size() != right.size())
	{
		return false;
	}
	// A command name is at least one character, so the first one decides most of
	// the comparisons the table makes before the loop is entered at all.
	if (left.empty() || FoldCase(left.front()) != FoldCase(right.front()))
	{
		return false;
	}
	for (std::size_t index = 1; index < left.size(); ++index)
	{
		if (FoldCase(left[index]) != FoldCase(right[index]))
		{
			return false;
		}
	}
	return true;
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

// The command a list of words asks for. A word that names no command is the
// error, not an exception: the client is told, and told what it said.
CommandValidation Validate(Arguments arguments)
{
	if (arguments.empty())
	{
		return Error("ERR command must be a non-empty RESP array");
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
} // namespace

CommandValidation ValidateCommand(const RESP::Object &request)
{
	const auto *array = std::get_if<RESP::Array>(&request.value);
	if (array == nullptr)
	{
		return Error("ERR command must be a non-empty RESP array");
	}

	std::vector<std::string_view> arguments;
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
	return Validate(arguments);
}

CommandValidation ValidateCommand(std::span<const std::string_view> arguments)
{
	return Validate(arguments);
}

bool IsWriteCommand(CommandType type) noexcept
{
	switch (type)
	{
	case CommandType::kSet:
	case CommandType::kDel:
	case CommandType::kExpire:
		return true;
	default:
		return false;
	}
}

bool IsStartupConfigParameter(std::string_view name) noexcept
{
	return name == "port" || name == "replication_address";
}

std::string_view CommandName(CommandType type)
{
	switch (type)
	{
	case CommandType::kPing:
		return "PING";
	case CommandType::kInfo:
		return "INFO";
	case CommandType::kGet:
		return "GET";
	case CommandType::kSet:
		return "SET";
	case CommandType::kDel:
		return "DEL";
	case CommandType::kExists:
		return "EXISTS";
	case CommandType::kDbSize:
		return "DBSIZE";
	case CommandType::kExpire:
		return "EXPIRE";
	case CommandType::kTTL:
		return "TTL";
	case CommandType::kMulti:
		return "MULTI";
	case CommandType::kExec:
		return "EXEC";
	case CommandType::kConfig:
		return "CONFIG";
	case CommandType::kCommand:
		return "COMMAND";
	case CommandType::kClient:
		return "CLIENT";
	case CommandType::kBgSave:
		return "BGSAVE";
	}
	return "";
}

namespace
{
RESP::Object Bulk(std::string value)
{
	return RESP::Object(RESP::BulkString{.value = std::move(value)});
}

// Everything a command encodes to is an array of bulk strings, but the rest of
// RESP is written out too so this stays a serializer instead of a special case
// that silently drops whatever it does not recognise.
void AppendRESP(std::string &out, const RESP::Object &object)
{
	if (const auto *array = std::get_if<RESP::Array>(&object.value))
	{
		out += '*';
		out += std::to_string(array->values.size());
		out += "\r\n";
		for (const RESP::Object &element : array->values)
		{
			AppendRESP(out, element);
		}
		return;
	}
	if (const auto *bulk = std::get_if<RESP::BulkString>(&object.value))
	{
		if (!bulk->value)
		{
			out += "$-1\r\n";
			return;
		}
		out += '$';
		out += std::to_string(bulk->value->size());
		out += "\r\n";
		out += *bulk->value;
		out += "\r\n";
		return;
	}
	if (const auto *simple = std::get_if<RESP::SimpleString>(&object.value))
	{
		out += '+';
		out += simple->value;
		out += "\r\n";
		return;
	}
	if (const auto *integer = std::get_if<RESP::Integer>(&object.value))
	{
		out += ':';
		out += std::to_string(integer->value);
		out += "\r\n";
		return;
	}
}
} // namespace

bool IsCommandName(std::string_view name, CommandType type) noexcept
{
	return SameCommand(name, CommandName(type));
}

RESP::Object CommandToRESP(const Command &command)
{
	RESP::Array array;
	auto push = [&array](std::string value) {
		array.values.push_back(Bulk(std::move(value)));
	};

	push(std::string(CommandName(command.type)));
	switch (command.type)
	{
	case CommandType::kPing:
	case CommandType::kInfo:
	case CommandType::kMulti:
	case CommandType::kExec:
	case CommandType::kBgSave:
	case CommandType::kCommand:
	case CommandType::kTTL:
	case CommandType::kDbSize:
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
	case CommandType::kConfig: {
		const auto &config = std::get<ConfigParams>(command.parameters);
		push(config.values.empty() ? "GET" : "SET");
		push(config.parameter);
		for (const std::string &value : config.values)
		{
			push(value);
		}
		break;
	}
	case CommandType::kClient: {
		const auto &clientInfo = std::get<ClientParams>(command.parameters);
		push(clientInfo.subcommand);
		for (const std::string &argument : clientInfo.arguments)
		{
			push(argument);
		}
		break;
	}
	}

	return RESP::Object(std::move(array));
}

std::string EncodeCommand(const Command &command)
{
	std::string bytes;
	bytes.reserve(64);
	AppendRESP(bytes, CommandToRESP(command));
	return bytes;
}
} // namespace KV::Command
