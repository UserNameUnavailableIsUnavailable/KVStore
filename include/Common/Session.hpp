#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "Common/Connection.hpp"

namespace KV
{
class Session
{
public:
    Session() = default;
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    Session(Session&&) noexcept = default;
    Session& operator=(Session&&) noexcept = default;

    Connection& GetConnection()
    {
        return connection_;
    }

    template <typename F>
    auto WithReceiveContext(F&& f)
    {
        receive_offset_ = read_buffer_.size();
        read_buffer_.resize(receive_offset_ + receive_chunk_size_);
        return f(connection_.GetFileDescriptor(), read_buffer_.data() + receive_offset_, receive_chunk_size_);
    }

    void CompleteReceive(int result)
    {
        const auto received = result > 0 ? static_cast<std::size_t>(result) : 0;
        read_buffer_.resize(receive_offset_ + received);
    }

    template <typename F>
    auto WithReadBuffer(F&& f) const
    {
        return f(static_cast<const std::vector<char>&>(read_buffer_));
    }

    bool ConsumeReadBuffer(std::size_t size)
    {
        const auto consumed = std::min(size, read_buffer_.size());
        read_buffer_.erase(read_buffer_.begin(), read_buffer_.begin() + static_cast<std::ptrdiff_t>(consumed));
        return !read_buffer_.empty();
    }

    void ClearReadBuffer()
    {
        read_buffer_.clear();
        receive_offset_ = 0;
    }

    template <typename F>
    auto WithWriteBuffer(F&& f)
    {
        write_offset_ = 0;
        return f(write_buffer_);
    }

    template <typename F>
    auto WithSendContext(F&& f) const
    {
        return f(connection_.GetFileDescriptor(), write_buffer_.data() + write_offset_, write_buffer_.size() - write_offset_);
    }

    bool CompleteSend(int result)
    {
        if (result > 0)
        {
            write_offset_ += static_cast<std::size_t>(result);
        }
        return write_offset_ == write_buffer_.size();
    }

    template <typename F>
    auto WithConstWriteBuffer(F&& f) const
    {
        return f(static_cast<const std::vector<char>&>(write_buffer_));
    }

    void Reset()
    {
        connection_.Reset();
        read_buffer_.clear();
        write_buffer_.clear();
        receive_offset_ = 0;
        write_offset_ = 0;
    }

private:
    static constexpr std::size_t receive_chunk_size_ = 4096;
    Connection connection_;
    std::vector<char> read_buffer_;
    std::vector<char> write_buffer_;
    std::size_t receive_offset_ = 0;
    std::size_t write_offset_ = 0;
};
} // namespace KV