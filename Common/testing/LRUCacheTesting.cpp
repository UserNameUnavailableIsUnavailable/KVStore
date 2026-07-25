#include <gtest/gtest.h>

#include "Common/Command.hpp"
#include "Common/LRUCache.hpp"

TEST(LRUCacheTesting, LRUCacheTesting)
{
	KV::LRUCache<std::string> lru(2);
	lru.Set("Foo", "Bar");
	lru.Set("Baz", "Qux");
	auto foo = lru.Get("Foo");
	lru.Set("Hello", "World");
	assert(!lru.Exists("Baz"));
}

TEST(CommandTesting, SerializesAndDeserializesCommand)
{
	const KV::Command expected("set", {"key", "value\r\nwith newline"});
	const std::string serialized = expected.Serialize();
	EXPECT_EQ(serialized, "3\r\n3\r\nset\r\n3\r\nkey\r\n19\r\nvalue\r\nwith newline\r\n");

	KV::Command actual;
	const auto parsed = actual.Deserialize(serialized);
	EXPECT_EQ(parsed.status, KV::CommandParseStatus::kOk);
	EXPECT_EQ(parsed.consumed_bytes, serialized.size());
	EXPECT_EQ(actual.GetName(), "SET");
	EXPECT_EQ(actual.GetArguments(), (std::vector<std::string> {"key", "value\r\nwith newline"}));
}

TEST(CommandTesting, ReportsIncompleteAndMalformedFrames)
{
	KV::Command command;
	EXPECT_EQ(command.Deserialize("2\r\n3\r\nGET\r\n5\r\nke").status,
		KV::CommandParseStatus::kIncomplete);

	const auto malformed = command.Deserialize("invalid\r\n");
	EXPECT_EQ(malformed.status, KV::CommandParseStatus::kProtocolError);
	EXPECT_EQ(malformed.error_message, "invalid token count");
}
