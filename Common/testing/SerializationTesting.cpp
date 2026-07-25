#include <gtest/gtest.h>

#include "Common/Result.hpp"

TEST(ResultTesting, SerializesAndDeserializesBulkString)
{
	const KV::Result expected(true, "", "value\r\nwith newline");
	const std::string serialized = expected.Serialize();

	EXPECT_EQ(serialized, "+OK\r\n$19\r\nvalue\r\nwith newline\r\n");

	KV::Result actual;
	actual.Deserialize(serialized);
	EXPECT_TRUE(actual.Ok());
	EXPECT_TRUE(actual.GetMessage().empty());
	EXPECT_EQ(actual.GetResult(), "value\r\nwith newline");
}

TEST(ResultTesting, SerializesErrorResponsesAndRejectsMalformedInput)
{
	EXPECT_EQ(KV::SerializeResult(false, "missing key", ""),
		"-ERR missing key\r\n$0\r\n\r\n");

	KV::Result result;
	result.Deserialize("+OK\r\n$4\r\ntwo\r\n");
	EXPECT_FALSE(result.Ok());
	EXPECT_EQ(result.GetMessage(), "protocol error");
	EXPECT_TRUE(result.GetResult().empty());
}
