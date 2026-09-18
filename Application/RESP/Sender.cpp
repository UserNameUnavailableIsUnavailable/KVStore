#include "Sender.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <stdexcept>

#include "RESP.hpp"

namespace RESP
{
Sender::Sender(Foundation::NBIO::Session &session, Foundation::Core::Buffer &buffer)
    : session_(session), buffer_(buffer)
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

void Sender::append(const Object &object)
{
    // Written straight into the buffer, which grows to hold it. Encoding is the
    // hot path of a server that answers a million replies a second, so it builds
    // nothing on the way: no string per number, no piece list, no coroutine.
    if (!RESP::AppendObject(object, buffer_))
    {
        throw std::runtime_error("reply batch is larger than the send buffer can hold");
    }
}

Foundation::NBIO::Task<bool> Sender::flush()
{
    while (!buffer_.is_empty())
    {
        auto result = co_await session_.send(buffer_.readable_span());
        if (!result)
        {
            internal_error_ = session_.send_channel().last_error().message();
            co_return false;
        }
        if (*result == 0)
        {
            internal_error_ = "stream closed";
            co_return false;
        }
        buffer_.consume(*result);
    }
    co_return true;
}

Foundation::NBIO::Task<bool> Sender::send(const Object &object)
{
    append(object);
    co_return co_await flush();
}
} // namespace RESP
