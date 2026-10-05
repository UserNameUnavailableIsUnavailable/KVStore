#pragma once

#include <Application/resp/resp.hpp>
#include <nbio/utility/buffer.hpp>
#include <nbio/net/tcp_socket.hpp>
#include <nbio/async/runtime.hpp>
#include <nbio/net/tcp_session_service.hpp>

namespace RESP {
class Sender {
   public:
    Sender(nbio::net::TcpSessionService& session, ::nbio::utility::Buffer& buffer);
    ~Sender() noexcept;

    // Encodes one reply into the buffer without writing it, so the replies that
    // are ready at the same time leave in one write. The buffer grows to hold the
    // batch: a reply that does not fit is made room for rather than written out
    // on its own, or the batch would be split anyway.
    void Append(const Object& object);

    // Writes everything that has been appended. False when the stream is gone.
    nbio::async::Task<bool> Flush();

    // One reply, written now. The answers that end a connection have nothing to
    // batch with, and neither does the first reply of a quiet one.
    nbio::async::Task<bool> Send(const Object& object);

    // How much of the batch is still to be written.
    std::size_t Pending() const noexcept { return buffer_.readable_size(); }

    std::size_t bytes_sent() const noexcept { return bytes_sent_; }

    std::string encode_error() const { return encode_error_; }

    std::string internal_error() const { return internal_error_; }

   private:
    nbio::net::TcpSessionService& session_;
    nbio::utility::Buffer& buffer_;
    std::size_t bytes_sent_{0};
    std::string encode_error_;
    std::string internal_error_;
};

}  // namespace RESP
