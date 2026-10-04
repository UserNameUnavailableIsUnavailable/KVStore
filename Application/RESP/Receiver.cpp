#include "Receiver.hpp"

#include <Application/RESP/RESP.hpp>
#include <nbio/utility/Buffer.hpp>
#include <nbio/net/TcpSocket.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/net/TcpSessionService.hpp>

namespace RESP {
namespace {
// What to make room for when the buffer is full. A request that arrives in one
// piece is read in as few passes as its own ceiling allows, and one that
// outgrows that ceiling is refused rather than waited on forever for input that
// cannot fit.
constexpr std::size_t kReadHeadroom = 16U * 1024U;
}  // namespace

Receiver::Receiver(nbio::net::TcpSessionService& session, ::nbio::utility::Buffer& buffer)
    : session_(session), buffer_(buffer) {}

Receiver::~Receiver() noexcept = default;

Decoder& Receiver::decoder() {
    if (!pending_) {
        pending_.emplace(Decode(buffer_, Dialect::kMultibulkAndInline));
    }
    return *pending_;
}

nbio::async::Task<nbio::runtime, std::optional<Object>> Receiver::Receive() {
    // The buffer belongs to the connection, not to this call. Whatever a
    // previous command left behind -- the rest of a pipeline, usually -- is
    // decoded before another read happens, so a client that sends its commands
    // back to back gets an answer to every one of them instead of having all but
    // the first dropped.
    no_command_ = false;
    Release();
    // A client may speak either dialect: a benchmark's PING_INLINE test writes a
    // line where a RESP client would write a multi-bulk.
    while (decoder().poll() == DecodeStatus::kNeedInput) {
        // A command cannot be decoded until all of it is here, so it has to be
        // able to make the buffer grow: reading into no space at all would stall
        // the connection until the client gave up.
        if (buffer_.writable_size() == 0 && !buffer_.reserve(kReadHeadroom)) {
            interal_error_ = "request larger than the receive buffer";
            co_return {};
        }

        auto result = co_await session_.Receive(buffer_.writable_span());
        if (!result) {
            interal_error_ = result.error().message();
            co_return {};
        }
        if (*result == 0) {
            // End of stream. A command that is still incomplete will never be
            // completed, so the connection is finished rather than waited on.
            interal_error_ = "stream closed";
            buffer_.clear();
            co_return {};
        }
        buffer_.commit(*result);
    }

    auto object = Finish(decoder().result());
    pending_.reset();
    co_return object;
}

std::optional<Object> Receiver::TryReceive() {
    // Only what is already here: no read, no wait. The caller that answers a
    // pipeline in one write asks this until it says there is nothing complete
    // left, and so never Parks with replies still in hand. A command that is only
    // half here stays half-decoded until the rest arrives.
    no_command_ = false;
    Release();
    if (decoder().poll() == DecodeStatus::kNeedInput) {
        return std::nullopt;
    }
    auto object = Finish(decoder().result());
    pending_.reset();
    return object;
}

void Receiver::Release() {
    buffer_.consume(borrowed_);
    borrowed_ = 0;
}

std::optional<Receiver::Command> Receiver::Take() {
    // The decoder reads the buffer as it goes: once it has started on a command,
    // the bytes of that command are not at the front of the buffer any more, so a
    // scan started now would begin in the middle of one and read its arguments as
    // commands. While the decoder is holding the rest of a command, it is the
    // only reader. It hands the command over and is dropped the moment one is
    // complete, so this is only the case for a command that is half here.
    if (!pending_) {
        std::size_t size = 0;
        switch (ScanCommand(buffer_, words_, size)) {
            case ScanStatus::kComplete:
                // Borrowed, not copied: the words point into the buffer and stay
                // valid until the command after this one is asked for.
                borrowed_ = size;
                return Command{.words = words_, .object = std::nullopt};
            case ScanStatus::kNeedInput:
                return std::nullopt;
            case ScanStatus::kNotACommand:
                break;
        }
    }

    // Not a command written the way a client writes one: an inline line, an
    // argument that is not a string, or bytes that are not a command at all. The
    // decoder reads every shape of the protocol and it is the one that says which
    // of them this is, so the bytes are left for it.
    if (decoder().poll() == DecodeStatus::kNeedInput) {
        return std::nullopt;
    }
    auto object = Finish(decoder().result());
    pending_.reset();
    if (!object) {
        return std::nullopt;
    }
    return Command{.words = {}, .object = std::move(object)};
}

nbio::async::Task<nbio::runtime, std::optional<Receiver::Command>> Receiver::ReceiveCommand() {
    no_command_ = false;
    // A protocol error is reported once: the caller was told what was wrong with
    // the bytes and the connection goes on, so the answer belongs to those bytes
    // and not to the command that comes after them.
    decode_error_.clear();
    Release();

    while (true) {
        if (auto command = Take()) {
            co_return command;
        }
        if (!decode_error_.empty() || no_command_ || !interal_error_.empty()) {
            co_return std::nullopt;
        }

        // A command cannot be read until all of it is here, so it has to be able
        // to make the buffer grow: reading into no space at all would stall the
        // connection until the client gave up.
        if (buffer_.writable_size() == 0 && !buffer_.reserve(kReadHeadroom)) {
            interal_error_ = "request larger than the receive buffer";
            co_return std::nullopt;
        }

        auto result = co_await session_.Receive(buffer_.writable_span());
        if (!result) {
            interal_error_ = result.error().message();
            co_return std::nullopt;
        }
        if (*result == 0) {
            // End of stream. A command that is still incomplete will never be
            // completed, so the connection is finished rather than waited on.
            interal_error_ = "stream closed";
            buffer_.clear();
            co_return std::nullopt;
        }
        buffer_.commit(*result);
    }
}

std::optional<Receiver::Command> Receiver::TryReceiveCommand() {
    no_command_ = false;
    decode_error_.clear();
    Release();
    return Take();
}

std::optional<Object> Receiver::Finish(DecodeResult& decoded) {
    if (decoded.status == DecodeStatus::kProtocolError) {
        // The bytes that did not parse are dropped: they cannot be retried, and
        // keeping them would make every later command fail on them too.
        decode_error_ = std::move(decoded.error);
        buffer_.clear();
        return std::nullopt;
    }

    if (!decoded.object) {
        // A complete line that named no command: an inline line with nothing on
        // it. It is consumed and ignored, which is what a client that sent a
        // blank line is owed.
        no_command_ = true;
        return std::nullopt;
    }

    return std::move(decoded.object);
}
}  // namespace RESP
