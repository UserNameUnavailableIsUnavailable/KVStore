#include <Foundation/Socket.hpp>

#include <gtest/gtest.h>

TEST(SocketTesting, OwnsAndTransfersNativeHandle)
{
    KVFoundation::Socket socket(KVFoundation::SocketProtocol::kTcp);
    const auto native_handle = socket.get_native_handle();

    ASSERT_GE(native_handle, 0);
    EXPECT_TRUE(socket.IsOpen());

    KVFoundation::Socket moved_socket(std::move(socket));

    EXPECT_FALSE(socket.IsOpen());
    EXPECT_TRUE(moved_socket.IsOpen());
    EXPECT_EQ(moved_socket.get_native_handle(), native_handle);
}

TEST(SocketTesting, BindsAnEphemeralPort)
{
    KVFoundation::Socket socket(KVFoundation::SocketProtocol::kTcp);
    socket.SetReuseAddress();
    EXPECT_NO_THROW(socket.Bind(0));
}
