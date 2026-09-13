#include "Sender.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <stdexcept>

#include "RESP.hpp"

namespace RESP
{
Sender::Sender(Foundation::NBIO::Session &session, Foundation::Core::Buffer &buffer, const Object &object)
    : session_(session), buffer_(buffer), object_(object)
{
    if (!buffer.is_empty())
    {
        throw std::logic_error("send buffer must be clean");
    }
}

Sender::~Sender() noexcept
{
    buffer_.clear();
}

Foundation::NBIO::Task<bool> Sender::send()
{
    auto decoder = RESP::Encode(object_, buffer_);
    while (decoder.poll() == RESP::EncodeStatus::kNeedFlush)
    {
        auto result = co_await session_.send(buffer_);
        if (result.status != Foundation::Core::SendStatus::kDone)
        {
            internal_error_ = result.error_code.message();
            co_return false;
        }
        bytes_sent_ += result.bytes_transferred;
    }
    if (!buffer_.is_empty())
    {
        auto result = co_await session_.send(buffer_);
        if (result.status != Foundation::Core::SendStatus::kDone)
        {
            internal_error_ = result.error_code.message();
            co_return false;
        }
        bytes_sent_ += result.bytes_transferred;
    }
    co_return true;
}
} // namespace RESP
