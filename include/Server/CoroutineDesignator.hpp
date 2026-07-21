#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace coro
{
class CoroutineTaskBase
{
public:
    virtual ~CoroutineTaskBase() = default;
    virtual bool Resume() = 0;
    virtual bool Done() const = 0;
    virtual void RethrowIfFailed() const = 0;
};

class NetworkTask final : public CoroutineTaskBase
{
public:
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    NetworkTask() = default;
    explicit NetworkTask(handle_type handle) noexcept : handle_(handle) {}

    NetworkTask(const NetworkTask&) = delete;
    NetworkTask& operator=(const NetworkTask&) = delete;

    NetworkTask(NetworkTask&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}

    NetworkTask& operator=(NetworkTask&& other) noexcept
    {
        if (this != &other)
        {
            if (handle_)
            {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, nullptr);
        }
        return *this;
    }

    ~NetworkTask() noexcept override
    {
        if (handle_)
        {
            handle_.destroy();
        }
    }

    bool Resume() override
    {
        if (!handle_ || handle_.done())
        {
            return false;
        }
        handle_.resume();
        return !handle_.done();
    }

    bool Done() const override
    {
        return !handle_ || handle_.done();
    }

    void RethrowIfFailed() const override;

private:
    handle_type handle_ = nullptr;
};

struct NetworkTask::promise_type
{
    std::exception_ptr exception;

    NetworkTask get_return_object() noexcept
    {
        return NetworkTask(handle_type::from_promise(*this));
    }

    std::suspend_always initial_suspend() noexcept
    {
        return {};
    }

    std::suspend_always final_suspend() noexcept
    {
        return {};
    }

    void return_void() noexcept {}

    void unhandled_exception() noexcept
    {
        exception = std::current_exception();
    }
};

inline void NetworkTask::RethrowIfFailed() const
{
    if (!handle_)
    {
        return;
    }
    if (handle_.promise().exception)
    {
        std::rethrow_exception(handle_.promise().exception);
    }
}

class TaskDesignator
{
public:
    using TaskId = std::uint64_t;

    TaskId Start(std::unique_ptr<CoroutineTaskBase> task)
    {
        if (!task)
        {
            throw std::invalid_argument("task must not be null");
        }

        const TaskId id = next_id_++;
        auto [it, inserted] = tasks_.emplace(id, std::move(task));
        if (!inserted)
        {
            throw std::runtime_error("failed to register coroutine task");
        }

        it->second->Resume();
        if (it->second->Done())
        {
            it->second->RethrowIfFailed();
            tasks_.erase(it);
        }
        return id;
    }

    TaskId Start(NetworkTask task)
    {
        return Start(std::make_unique<NetworkTask>(std::move(task)));
    }

    bool Resume(TaskId id)
    {
        auto it = tasks_.find(id);
        if (it == tasks_.end())
        {
            return false;
        }

        it->second->Resume();
        if (it->second->Done())
        {
            it->second->RethrowIfFailed();
            tasks_.erase(it);
            return false;
        }
        return true;
    }

    void ResumeAll()
    {
        for (auto it = tasks_.begin(); it != tasks_.end();)
        {
            it->second->Resume();
            if (it->second->Done())
            {
                it->second->RethrowIfFailed();
                it = tasks_.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

    std::size_t ActiveCount() const noexcept
    {
        return tasks_.size();
    }

private:
    TaskId next_id_ = 1;
    std::unordered_map<TaskId, std::unique_ptr<CoroutineTaskBase>> tasks_;
};
} // namespace coro
