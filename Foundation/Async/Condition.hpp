#pragma once

#include <Foundation/Async/Task.hpp>
#include <Foundation/Async/NotifyChannel.hpp>
#include <coroutine>
#include <deque>
#include <mutex>

namespace Foundation::Async::detail
{
class Condition;
struct ConditionAwaiter
{
    ConditionAwaiter(Condition& notification);
    bool await_ready() noexcept;
    void await_suspend(std::coroutine_handle<> h);
    void await_resume() noexcept;

    ~ConditionAwaiter() noexcept;
    std::coroutine_handle<> handle{};
    Condition& condition;
};

class Condition
{
public:
    Condition(NotifyChannel& channel);
    ~Condition() noexcept;
    void notify_one();
    void notify_all();
    ConditionAwaiter wait();
    template <typename Predicate>
    Task<void> wait(Predicate predicate);
private:
    // others cannot call insert & remove
    friend ConditionAwaiter;
    void insert(std::coroutine_handle<> h);
    bool remove(std::coroutine_handle<> h);
    std::mutex mutex_;
    NotifyChannel& channel_;
    // continuous storage is typically faster than listing
    std::deque<std::coroutine_handle<>> notifiees_;
};

template <typename Predicate>
Task<void> Condition::wait(Predicate predicate)
{
    while (!predicate())
    {
        co_await wait();
    }
}
} // namespace Foundation::Async