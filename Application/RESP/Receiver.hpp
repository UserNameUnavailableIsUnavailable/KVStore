#pragma once

#include <Application/RESP/RESP.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/net/TcpSessionService.hpp>
#include <cstddef>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace RESP {
class Receiver {
   public:
    // A command from a client, in whichever shape it was written.
    struct Command {
        // The words of the command when they could be read where they lie: views
        // into the receive buffer, valid until the next command is read. Nothing
        // was copied to have them.
        std::span<const std::string_view> words;
        // The command as an object, when it could not be read in place: an inline
        // line, an argument that is not a string, a malformed one the decoder has
        // to have a look at. Built by the decoder, which reads every shape of the
        // protocol, so it is owned rather than borrowed.
        std::optional<Object> object;
    };

    Receiver(nbio::net::TcpSessionService& session, ::nbio::utility::Buffer& buffer);
    ~Receiver() noexcept;

    nbio::async::Task<nbio::runtime, std::optional<Object>> Receive();

    // Decodes only what is already buffered: no read, no wait. A caller that
    // wants to answer a pipeline in one write asks this until it reports that no
    // complete command is left, so it never Parks with replies still in hand.
    std::optional<Object> TryReceive();

    // The same two, for a server reading commands rather than a client reading
    // replies. A command written the way a client writes one -- an array of bulk
    // strings -- is read where it lies, as words pointing into the receive
    // buffer, and a GET costs one pass and no allocation. Anything else is handed
    // to the decoder above.
    nbio::async::Task<nbio::runtime, std::optional<Command>> ReceiveCommand();
    std::optional<Command> TryReceiveCommand();

    std::string decode_error() const { return decode_error_; }
    std::string internal_error() const { return interal_error_; }
    // True when what arrived held no command at all: an inline line with no
    // words on it, which redis answers nothing to. It is not an error, so the
    // connection goes on rather than being reported and closed.
    bool no_command() const { return no_command_; }

   private:
    // Lets go of the bytes of the command that was handed out: its words are
    // views into them, so nothing may be taken out of the buffer until the caller
    // that borrowed them has finished with them. The next command is the first
    // thing that may write over them, which is why this is the first thing read
    // does.
    void Release();

    // One attempt at a command that may already be in the buffer: read where it
    // lies when it can be, decoded when it cannot, nothing when there is not
    // enough of it yet.
    std::optional<Command> Take();

    // What a decoder's outcome means: a protocol error is recorded, a line with
    // no command is remembered as nothing owed, and a command is handed over.
    std::optional<Object> Finish(DecodeResult& decoded);

    // The decoder for the command being read, created on first use. It is kept
    // between calls on purpose: a decoder abandoned half way through a command
    // has already taken those bytes out of the buffer, so a fresh one would
    // start in the middle of that command and read its arguments as commands.
    Decoder& decoder();

    nbio::net::TcpSessionService& session_;
    nbio::utility::Buffer& buffer_;
    std::optional<Decoder> pending_;
    // The words of the last command handed out, and how many bytes of the buffer
    // they are views into. Both belong to the connection rather than to a call,
    // so a pipeline is read through one vector and a borrow is handed back once.
    std::vector<std::string_view> words_;
    std::size_t borrowed_{0};
    std::string decode_error_;
    std::string interal_error_;
    bool no_command_ = false;
};
}  // namespace RESP
