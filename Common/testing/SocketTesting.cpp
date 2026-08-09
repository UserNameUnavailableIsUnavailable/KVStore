#include "Common/Socket.hpp"

#include <gtest/gtest.h>

TEST(SocketTesting, OwnsAndTransfersNativeHandle)
{
    KV::Socket socket(KV::SocketProtocol::kTcp);
    const auto native_handle = socket.GetNativeHandle();

    ASSERT_GE(native_handle, 0);
    EXPECT_TRUE(socket.IsOpen());

    KV::Socket moved_socket(std::move(socket));

    EXPECT_FALSE(socket.IsOpen());
    EXPECT_TRUE(moved_socket.IsOpen());
    EXPECT_EQ(moved_socket.GetNativeHandle(), native_handle);
}

TEST(SocketTesting, BindsAnEphemeralPort)
{
    KV::Socket socket(KV::SocketProtocol::kTcp);
    socket.SetReuseAddress();
    EXPECT_NO_THROW(socket.Bind(0));
}