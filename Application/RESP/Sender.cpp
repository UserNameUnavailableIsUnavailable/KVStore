#include "Sender.hpp"

#include <nbio/async/Task.hpp>
#include <nbio/utility/Buffer.hpp>
#include <nbio/net/TcpSocket.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <stdexcept>

#include "RESP.hpp"

namespace RESP {
Sender::Sender(nbio::net::TcpSessionService& session, nbio::utility::Buffer& buffer)
    : session_(session), buffer_(buffer) {
    if (!buffer.is_empty()) {
        throw std::logic_error("send buffer must be clean");
    }
}

Sender::~Sender() noexcept { buffer_.clear(); }

void Sender::Append(const Object& object) {
    // Written straight into the buffer, which grows to hold it. Encoding is the
    // hot path of a server that answers a million replies a second, so it builds
    // nothing on the way: no string per number, no piece list, no coroutine.
    if (!RESP::AppendObject(object, buffer_)) {
        throw std::runtime_error("reply batch is larger than the send buffer can hold");
    }
}

nbio::async::Task<nbio::runtime, bool> Sender::Flush() {
    while (!buffer_.is_empty()) {
        auto result = co_await session_.Send(buffer_.readable_span());
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

nbio::async::Task<nbio::runtime, bool> Sender::Send(const Object& object) {
    Append(object);
    co_return co_await Flush();
}
}  // namespace RESP
