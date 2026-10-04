#pragma once

#include <Application/RESP/RESP.hpp>
#include <NBIO/Utility/Buffer.hpp>
#include <NBIO/Net/TcpSocket.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/Net/TcpSessionService.hpp>

namespace RESP {
class Sender {
   public:
    Sender(NBIO::Net::TcpSessionService& session, ::NBIO::Utility::Buffer& buffer);
    ~Sender() noexcept;

    // Encodes one reply into the buffer without writing it, so the replies that
    // are ready at the same time leave in one write. The buffer grows to hold the
    // batch: a reply that does not fit is made room for rather than written out
    // on its own, or the batch would be split anyway.
    void append(const Object& object);

    // Writes everything that has been appended. False when the stream is gone.
    NBIO::Async::Task<NBIO::Runtime, bool> flush();

    // One reply, written now. The answers that end a connection have nothing to
    // batch with, and neither does the first reply of a quiet one.
    NBIO::Async::Task<NBIO::Runtime, bool> send(const Object& object);

    // How much of the batch is still to be written.
    std::size_t pending() const noexcept { return buffer_.readable_size(); }

    std::size_t bytes_sent() const noexcept { return bytes_sent_; }

    std::string encode_error() const { return encode_error_; }

    std::string internal_error() const { return internal_error_; }

   private:
    NBIO::Net::TcpSessionService& session_;
    NBIO::Utility::Buffer& buffer_;
    std::size_t bytes_sent_{0};
    std::string encode_error_;
    std::string internal_error_;
};

}  // namespace RESP
