#pragma once

#include <memory_resource>
#include <algorithm>
#include <array>
#include <bitset>
#include <chrono>
#include <cstddef>
#include <memory>
#include <memory_resource>
#include <list>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "Common/Address.hpp"
#include "Common/Protocol.hpp"
#include "Common/Socket.hpp"

namespace KV
{
// Networking model a session is driven by. Selecting the model is a
// responsibility of the session: the Reactor model reacts to readiness
// notifications (epoll + coroutines), whereas the Proactor model reacts to
// completion notifications (io_uring).
enum class NetworkingModel
{
    kReactor,
    kProactor
};

// Outcome of asking a session to resolve the next request from the bytes it
// has buffered. This is the single vocabulary the networking models speak when
// they want to know what to do next, independent of how the bytes arrived.
enum class RequestStatus
{
    kNeedMore, // more bytes are required before a request can be resolved
    kReady, // a complete command was resolved and is ready to execute
    kProtocolError // the buffered bytes violate the wire protocol
};

// A resolved request handed back by Session::ConsumeRequest.
struct ResolvedRequest
{
    RequestStatus status = RequestStatus::kNeedMore;
    Command command;
    std::pmr::string error;
};

class Session
{
public:
	struct HandleType
	{
		std::uint32_t slot;
		std::uint32_t generation;
	};
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    Session(Session&&) noexcept = default;
    Session& operator=(Session&&) = delete;
    virtual ~Session() = 0;

    NetworkingModel GetNetworkingModel() const
    {
        return networking_model_;
    }

    std::pmr::memory_resource* GetMemoryResource() const
    {
        return resource_;
    }

    void UseMemoryResource(std::pmr::memory_resource* resource)
    {
        resource_ = resource;
        std::destroy_at(&read_buffer_);
        std::construct_at(&read_buffer_, resource_);
        std::destroy_at(&write_buffer_);
        std::construct_at(&write_buffer_, resource_);
        std::destroy_at(&transaction_);
        std::construct_at(&transaction_, resource_);
        std::destroy_at(&protocol_);
        std::construct_at(&protocol_, resource_);
        read_offset_ = 0;
        write_offset_ = 0;
    }

    Socket& GetSocket()
    {
        return socket_;
    }

    const Socket& GetSocket() const
    {
        return socket_;
    }

    void AttachSocket(Socket::HandleType h)
    {
        if (socket_.GetNativeHandle() == h)
        {
            return;
        }
        socket_ = Socket::Adopt(h);
    }

    void AttachSocket(Socket&& socket) noexcept
    {
        std::swap(socket_, socket);
    }

    std::string GetPeerIP() const
    {
        return peer_address_.GetIP();
    }

    std::uint16_t GetPeerPort() const
    {
        return peer_address_.GetPort();
    }

    void SetTimeout(std::chrono::milliseconds timeout)
    {
        timeout_ = timeout;
    }

    std::chrono::milliseconds GetTimeout() const
    {
        return timeout_;
    }

    // The accept path needs a mutable Address to populate the peer endpoint.
    template <typename F>
    auto WithMutableAcceptContext(F&& f)
    {
        peer_address_.Reset();
        return f(peer_address_);
    }

    // Protocol resolution ---------------------------------------------------

    // Drive the protocol state machine against the bytes currently buffered.
    // The networking model only has to feed bytes in (PrepareReceive /
    // CompleteReceive) and then ask the session what to do next:
    //   - kNeedMore      : keep receiving; the read buffer is left intact.
    //   - kReady         : execute ResolvedRequest::command, then PrepareResponse.
    //   - kProtocolError : reply with the error; the read buffer has been drained.
    ResolvedRequest ConsumeRequest()
    {
        RequestDecode parsed = ResolveRequest();
        switch (parsed.status)
        {
            case DecodeStatus::kIncomplete:
                return {.status = RequestStatus::kNeedMore, .command = {}, .error = std::pmr::string(resource_)};
            case DecodeStatus::kProtocolError:
                ClearReadBuffer();
                return {.status = RequestStatus::kProtocolError, .command = {}, .error = std::move(parsed.error)};
            case DecodeStatus::kComplete:
                break;
        }

        const bool has_extra_data = ConsumeReadBuffer(parsed.consumed_bytes);
        if (has_extra_data)
        {
            // Pipelined requests are not supported yet: drain and reject so the
            // client gets a deterministic error instead of a desynchronised stream.
            ClearReadBuffer();
            return {.status = RequestStatus::kProtocolError,
                .command = {},
                .error = std::pmr::string("pipelined commands are not supported", resource_)};
        }

        ResolvedRequest resolved;
        resolved.status = RequestStatus::kReady;
        resolved.command = std::move(parsed.command);
        return resolved;
    }

    template <typename Execute>
    Result Process(Command command, Execute&& execute)
    {
        if (command.name == "MULTI")
        {
            if (in_transaction_)
            {
                return MakeError("MULTI calls can not be nested");
            }
            if (!command.arguments.empty())
            {
                return MakeError("MULTI takes no arguments");
            }
            in_transaction_ = true;
            return MakeSimple("OK");
        }

        if (!in_transaction_)
        {
            return execute(command);
        }

        if (command.name != "EXEC")
        {
            transaction_.push_back(std::move(command));
            return MakeSimple("QUEUED");
        }

        if (!command.arguments.empty())
        {
            return MakeError("EXEC takes no arguments");
        }

        Result response {.type = ResultType::kArray,
            .value = std::pmr::string(resource_),
            .elements = std::pmr::vector<Result>(resource_)};
        response.elements.reserve(transaction_.size());
        for (const Command& queued : transaction_)
        {
            response.elements.push_back(execute(queued));
        }
        transaction_.clear();
        in_transaction_ = false;
        return response;
    }

    template <typename F>
    auto WithWriteBuffer(F&& f)
    {
        write_offset_ = 0;
        return f(write_buffer_);
    }

    // Encode a response into the write buffer, ready for the model to flush.
    void PrepareResponse(const Result& result)
    {
        const std::pmr::string encoded = protocol_.EncodeResponse(result);
        WithWriteBuffer([&encoded](std::pmr::vector<char>& buffer) {
            buffer.assign(encoded.begin(), encoded.end());
        });
    }

    std::span<char> PrepareReceive()
    {
        read_offset_ = read_buffer_.size();
        read_buffer_.resize(read_offset_ + receive_chunk_size_);
        return {read_buffer_.data() + read_offset_, receive_chunk_size_};
    }

    std::span<const std::byte> CompleteReceive(int result)
    {
        const auto received = result > 0 ? static_cast<std::size_t>(result) : 0;
        read_buffer_.resize(read_offset_ + received);
        return {reinterpret_cast<const std::byte*>(read_buffer_.data() + read_offset_), received};
    }

    std::span<const char> GetPendingSend() const
    {
        return {write_buffer_.data() + write_offset_, write_buffer_.size() - write_offset_};
    }

    void PrepareRawWrite(std::string_view data)
    {
        WithWriteBuffer([data](std::pmr::vector<char>& buffer) {
            buffer.assign(data.begin(), data.end());
        });
    }

    void DiscardReadBuffer()
    {
        ClearReadBuffer();
    }

    bool CompleteSend(int result)
    {
        if (result > 0)
        {
            write_offset_ += static_cast<std::size_t>(result);
        }
        return write_offset_ == write_buffer_.size();
    }

    template <typename F>
    auto WithConstWriteBuffer(F&& f) const
    {
        return f(static_cast<const std::pmr::vector<char>&>(write_buffer_));
    }

    // Return the session to a pristine state: close the socket, forget the
    // peer endpoint, and drop every byte of protocol state.
    void Reset()
    {
        socket_ = Socket{};
        peer_address_.Reset();
        timeout_ = std::chrono::milliseconds{0};
        read_buffer_.clear();
        write_buffer_.clear();
        transaction_.clear();
        in_transaction_ = false;
        read_offset_ = 0;
        write_offset_ = 0;
    }

protected:
    // Only concrete derived sessions may be constructed, and each must declare
    // its networking model, which then stays fixed for the session's lifetime.
    explicit Session(NetworkingModel networking_model,
        std::pmr::memory_resource* resource = std::pmr::get_default_resource()) :
        networking_model_(networking_model),
        resource_(resource),
        read_buffer_(resource_),
        write_buffer_(resource_),
        protocol_(resource_),
        transaction_(resource_)
    {
    }

private:
    // Attempt to resolve a single request from the bytes currently buffered,
    // without mutating the buffer. ConsumeRequest layers the buffer bookkeeping
    // and pipelining policy on top of this.
    template <typename F>
    auto WithReadBuffer(F&& f) const
    {
        return f(static_cast<const std::pmr::vector<char>&>(read_buffer_));
    }

    RequestDecode ResolveRequest() const
    {
        return WithReadBuffer([this](const std::pmr::vector<char>& buffer) {
            return protocol_.DecodeRequest(std::string_view(buffer.data(), buffer.size()));
        });
    }

    Result MakeSimple(std::string_view value) const
    {
        return {.type = ResultType::kSimpleString,
            .value = std::pmr::string(value, resource_),
            .elements = std::pmr::vector<Result>(resource_)};
    }

    Result MakeError(std::string_view value) const
    {
        return {.type = ResultType::kError,
            .value = std::pmr::string(value, resource_),
            .elements = std::pmr::vector<Result>(resource_)};
    }

    // Drop the first `size` bytes of the read buffer. Returns whether any bytes
    // remain afterwards, which the caller uses to detect trailing (pipelined) data.
    bool ConsumeReadBuffer(std::size_t size)
    {
        const auto consumed = std::min(size, read_buffer_.size());
        read_buffer_.erase(read_buffer_.begin(), read_buffer_.begin() + static_cast<std::ptrdiff_t>(consumed));
        return !read_buffer_.empty();
    }

    void ClearReadBuffer()
    {
        read_buffer_.clear();
        read_offset_ = 0;
    }

    static constexpr std::size_t receive_chunk_size_ = 4096;
    const NetworkingModel networking_model_;
    std::pmr::memory_resource* resource_ = std::pmr::get_default_resource();
    Socket socket_;
    Address peer_address_;
    std::chrono::milliseconds timeout_{0};
    std::pmr::vector<char> read_buffer_{resource_};
    std::pmr::vector<char> write_buffer_{resource_};
    Protocol protocol_{resource_};
    std::pmr::vector<Command> transaction_{resource_};
    bool in_transaction_ = false;
    std::size_t read_offset_ = 0;
    std::size_t write_offset_ = 0;
};

// A pure virtual destructor still needs a definition, since derived
// destructors invoke it as part of their destruction sequence.
inline Session::~Session() = default;
} // namespace KV
