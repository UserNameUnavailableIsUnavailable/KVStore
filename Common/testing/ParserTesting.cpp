#include <gtest/gtest.h>

#include <string_view>

#include "Common/Command.hpp"
#include "Common/Parser.hpp"
#include "Common/Result.hpp"

TEST(ParserTesting, RoundTripsARequest)
{
	const KV::Parser parser;
	const KV::Command command("SET", {"key", "value"});

	const std::string serialized = parser.SerializeRequest(command);
	const KV::RequestParse parsed = parser.ParseRequest(serialized);

	EXPECT_EQ(parsed.status, KV::ProtocolStatus::kOk);
	EXPECT_EQ(parsed.consumed_bytes, serialized.size());
	EXPECT_EQ(parsed.command.GetName(), "SET");
	ASSERT_EQ(parsed.command.GetArguments().size(), 2u);
	EXPECT_EQ(parsed.command.GetArguments()[0], "key");
	EXPECT_EQ(parsed.command.GetArguments()[1], "value");
}

TEST(ParserTesting, ReportsIncompleteRequest)
{
	const KV::Parser parser;
	const KV::Command command("GET", {"key"});

	std::string serialized = parser.SerializeRequest(command);
	serialized.pop_back(); // truncate the trailing terminator

	const KV::RequestParse parsed = parser.ParseRequest(serialized);
	EXPECT_EQ(parsed.status, KV::ProtocolStatus::kIncomplete);
}

TEST(ParserTesting, ReportsProtocolErrorOnInvalidRequest)
{
	const KV::Parser parser;

	const KV::RequestParse parsed = parser.ParseRequest("not-a-number\r\n");
	EXPECT_EQ(parsed.status, KV::ProtocolStatus::kProtocolError);
	EXPECT_FALSE(parsed.error_message.empty());
}

TEST(ParserTesting, RoundTripsAResponse)
{
	const KV::Parser parser;
	const KV::Result result(true, "", "value");

	const std::string serialized = parser.SerializeResponse(result);
	const KV::ResponseParse parsed = parser.ParseResponse(serialized);

	EXPECT_EQ(parsed.status, KV::ProtocolStatus::kOk);
	EXPECT_TRUE(parsed.result.Ok());
	EXPECT_EQ(parsed.result.GetResult(), "value");
}
