#include "Common/Task.hpp"

#include <gtest/gtest.h>

namespace
{
KV::SessionTask CompletedTask()
{
    co_return;
}
} // namespace

TEST(TaskTesting, OwnsCompletedSessionCoroutine)
{
    auto task = CompletedTask();

    EXPECT_TRUE(task.Done());
    // Destructor handles cleanup.  I/O errors are handled via return values,
    // not exceptions — a clean co_return simply reaches Done() without ever
    // hitting unhandled_exception.
}
