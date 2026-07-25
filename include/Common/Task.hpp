#pragma once

#include <unistd.h>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <netinet/in.h>

namespace KV
{
enum class IOTaskType
{
    kNone,
    kAccept,
    kRecv,
    kSend,
};

class IOTask
{
public:
    IOTask() = default;
    IOTask(const IOTask&) = delete;
    IOTask& operator=(const IOTask&) = delete;
    ~IOTask()
    {
        if (fd_ >= 0)
        {
            ::close(fd_);
        }
    }
    IOTaskType GetType() const
    {
        return type_;
    }
    void SetType(IOTaskType type)
    {
        type_ = type;
    }
    bool IsMultishot() const
    {
        return is_multishot_;
    }
    void SetMultishot(bool is_multishot)
    {
        is_multishot_ = is_multishot;
    }
    int GetFileDescriptor() const
    {
        return fd_;
    }
    void SetFileDescriptor(int fd)
    {
        if (fd_ >= 0 && fd_ != fd)
        {
            ::close(fd_);
        }
        fd_ = fd;
    }
    void Reset()
    {
        if (fd_ >= 0)
        {
            ::close(fd_);
        }
        fd_ = -1;
        type_ = IOTaskType::kNone;
        is_multishot_ = false;
        read_buffer_.clear();
        write_buffer_.clear();
        receive_offset_ = 0;
        write_offset_ = 0;
        address_length_ = sizeof(address_);
    }
    template <typename F>
    auto WithMutableAcceptContext(F&& f)
    {
        address_length_ = sizeof(address_);
        return f(&address_, &address_length_);        
    }
    template <typename F>
    auto WithReceiveContext(F&& f)
    {
        receive_offset_ = read_buffer_.size();
        read_buffer_.resize(receive_offset_ + receive_chunk_size_);
        return f(fd_, read_buffer_.data() + receive_offset_, receive_chunk_size_);
    }
    void CompleteReceive(int result)
    {
        const auto received = result > 0 ? static_cast<std::size_t>(result) : 0;
        read_buffer_.resize(receive_offset_ + received);
    }
    template <typename F>
    auto WithMutableReadBuffer(F&& f)
    {
        return f(read_buffer_);
    }
    template <typename F>
    auto WithConstReadBuffer(F&& f) const
    {
        return f(static_cast<const std::vector<char>&>(read_buffer_));
    }
    template <typename F>
    auto WithMutableWriteBuffer(F&& f)
    {
        write_offset_ = 0;
        return f(write_buffer_);
    }
    template <typename F>
    auto WithSendContext(F&& f) const
    {
        return f(fd_, write_buffer_.data() + write_offset_, write_buffer_.size() - write_offset_);
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
private:
    std::intptr_t fd_ = -1; // the associated file descriptor for the task
    IOTaskType type_ = IOTaskType::kNone; // the type of the task (accept, read, write, or none)
    bool is_multishot_ = false; // indicates whether the task is a multishot operation (e.g., accept) or a single-shot operation (e.g., read/write)
    static constexpr std::size_t receive_chunk_size_ = 4096;
    std::vector<char> read_buffer_;
    std::vector<char> write_buffer_;
    std::size_t receive_offset_ = 0;
    std::size_t write_offset_ = 0;
    ::sockaddr_storage address_;
    ::socklen_t address_length_ = sizeof(::sockaddr_storage);
};
}