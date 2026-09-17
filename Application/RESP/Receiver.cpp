#include <stdexcept>
#include <Foundation/NBIO/Runtime.hpp>

#include "Receiver.hpp"

#include <Foundation/NBIO/Session.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>

#include <Application/RESP/RESP.hpp>

namespace RESP
{
namespace
{
// What to make room for when the buffer is full. A request that arrives in one
// piece is read in as few passes as its own ceiling allows, and one that
// outgrows that ceiling is refused rather than waited on forever for input that
// cannot fit.
constexpr std::size_t kReadHeadroom = 16U * 1024U;
} // namespace

Receiver::Receiver(Foundation::NBIO::Session &session, ::Foundation::Core::Buffer &buffer) : session_(session), buffer_(buffer)
{
}

Receiver::~Receiver() noexcept = default;

Foundation::NBIO::Task<std::optional<Object>> Receiver::receive()
{
    // The buffer belongs to the connection, not to this call. Whatever a
    // previous command left behind -- the rest of a pipeline, usually -- is
    // decoded before another read happens, so a client that sends its commands
    // back to back gets an answer to every one of them instead of having all but
    // the first dropped.
    auto decoder = Decode(buffer_);
    while (decoder.poll() == DecodeStatus::kNeedInput)
    {
        // A command cannot be decoded until all of it is here, so it has to be
        // able to make the buffer grow: reading into no space at all would stall
        // the connection until the client gave up.
        if (buffer_.writable_size() == 0 && !buffer_.reserve(kReadHeadroom))
        {
            interal_error_ = "request larger than the receive buffer";
            co_return {};
        }

        auto result = co_await session_.receive(buffer_.writable_span());
        if (!result)
        {
            interal_error_ = session_.receive_channel().last_error().message();
            co_return {};
        }
        if (*result == 0)
        {
            // End of stream. A command that is still incomplete will never be
            // completed, so the connection is finished rather than waited on.
            interal_error_ = "stream closed";
            buffer_.clear();
            co_return {};
        }
        buffer_.commit(*result);
    }

    if (decoder.status() == DecodeStatus::kProtocolError)
    {
        // The bytes that did not parse are dropped: they cannot be retried, and
        // keeping them would make every later command fail on them too.
        decode_error_ = std::move(decoder.result().error);
        buffer_.clear();
        co_return {};
    }

    co_return {std::move(decoder.result().object)};
}
} // namespace RESP
