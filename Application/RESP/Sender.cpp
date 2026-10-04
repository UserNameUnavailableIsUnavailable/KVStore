#include "Sender.hpp"

#include <NBIO/Async/Task.hpp>
#include <NBIO/Utility/Buffer.hpp>
#include <NBIO/Net/TcpSocket.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <stdexcept>

#include "RESP.hpp"

namespace RESP {
Sender::Sender(NBIO::Net::TcpSessionService& session, NBIO::Utility::Buffer& buffer)
    : session_(session), buffer_(buffer) {
    if (!buffer.is_empty()) {
        throw std::logic_error("send buffer must be clean");
    }
}

Sender::~Sender() noexcept { buffer_.clear(); }

void Sender::append(const Object& object) {
    // Written straight into the buffer, which grows to hold it. Encoding is the
    // hot path of a server that answers a million replies a second, so it builds
    // nothing on the way: no string per number, no piece list, no coroutine.
    if (!RESP::AppendObject(object, buffer_)) {
        throw std::runtime_error("reply batch is larger than the send buffer can hold");
    }
}

NBIO::Async::Task<NBIO::Runtime, bool> Sender::flush() {
    while (!buffer_.is_empty()) {
        auto result = co_await session_.send(buffer_.readable_span());
        if (!result) {
            internal_error_ = result.error().message();
            co_return false;
        }
        if (*result == 0) {
            internal_error_ = "stream closed";
            co_return false;
        }
        buffer_.consume(*result);
    }
    co_return true;
}

NBIO::Async::Task<NBIO::Runtime, bool> Sender::send(const Object& object) {
    append(object);
    co_return co_await flush();
}
}  // namespace RESP
