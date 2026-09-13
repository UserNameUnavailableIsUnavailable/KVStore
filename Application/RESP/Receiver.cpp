#include <stdexcept>
#include <Foundation/NBIO/Runtime.hpp>

#include "Receiver.hpp"

#include <Foundation/NBIO/Session.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>

#include <Application/RESP/RESP.hpp>

namespace RESP
{
Receiver::Receiver(Foundation::NBIO::Session &session, ::Foundation::Core::Buffer &buffer) : session_(session), buffer_(buffer)
{
    if (!buffer.is_empty())
    {
        throw std::logic_error("buffer is not empty");
    }
}

Receiver::~Receiver() noexcept
{
    buffer_.clear();
}

Foundation::NBIO::Task<std::optional<Object>> Receiver::receive()
{
    auto decoder = Decode(buffer_);
    do
    {
        auto result = co_await session_.receive(buffer_);
        if (result.status != Foundation::Core::ReceiveStatus::kDone)
        {
            internal_error_ = result.error_code.message();
            co_return {};
        }
        bytes_received_ += result.bytes_transferred;
    } while (decoder.poll() == DecodeStatus::kNeedInput);

    if (decoder.status() == DecodeStatus::kProtocolError)
    {
        decode_error_ = std::move(decoder.result().error);
        co_return {};
    }

    co_return {std::move(decoder.result().object)};
}
} // namespace RESP
