#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "Common/Connection.hpp"
#include "Common/Parser.hpp"

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
    Command command; // populated when status == kReady
    std::string error_message; // populated when status == kProtocolError
};

// A Session manages the lifetime of a single client conversation. It is the
// hub of the Session Layer described in DESIGN.md and provides:
//   - connection control (via the owned Connection),
//   - networking-model selection (Reactor / Proactor),
//   - protocol resolution (via the owned Parser).
//
// Every byte the session buffers is allocated through a pluggable
// std::pmr::memory_resource. This realises the Memory Pooling Layer from
// DESIGN.md: swapping the resource (new/delete, monotonic buffer, pool, or a
// custom allocator) lets us compare memory-pool implementations without
// touching any of the I/O or protocol code. The resource is owned by the
// caller and must outlive the session.
class Session
{
public:
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    Session(Session&&) noexcept = default;
    // Session is an abstract, polymorphic base. Assigning one session over
    // another risks slicing, and because networking_model_ is const a move
    // assignment cannot be defaulted anyway, so assignment is disabled.
    Session& operator=(Session&&) = delete;
    // Pure virtual destructor makes Session abstract: it can only be created
    // through a concrete derived session, which fixes the networking model.
    virtual ~Session() = 0;

    NetworkingModel GetNetworkingModel() const
    {
        return networking_model_;
    }

    // Memory pooling --------------------------------------------------------

    std::pmr::memory_resource* GetMemoryResource() const
    {
        return resource_;
    }

    // Rebind the session onto a different memory resource. A pmr allocator is
    // fixed at construction, so the buffers are reconstructed in place; this is
    // only valid before the session starts buffering (buffers must be empty),
    // which is why it is called once when a session is (re)started.
    void UseMemoryResource(std::pmr::memory_resource* resource)
    {
        resource_ = resource;
        std::destroy_at(&read_buffer_);
        std::construct_at(&read_buffer_, resource_);
        std::destroy_at(&write_buffer_);
        std::construct_at(&write_buffer_, resource_);
        receive_offset_ = 0;
        write_offset_ = 0;
    }

    // Connection control ----------------------------------------------------

    Connection& GetConnection()
    {
        return connection_;
    }

    const Connection& GetConnection() const
    {
        return connection_;
    }

    // Protocol resolution ---------------------------------------------------

    const Parser& GetParser() const
    {
        return parser_;
    }

    // Drive the protocol state machine against the bytes currently buffered.
    // The networking model only has to feed bytes in (WithReceiveContext /
    // CompleteReceive) and then ask the session what to do next:
    //   - kNeedMore      : keep receiving; the read buffer is left intact.
    //   - kReady         : execute ResolvedRequest::command, then PrepareResponse.
    //   - kProtocolError : reply with the error; the read buffer has been drained.
    ResolvedRequest ConsumeRequest()
    {
        RequestParse parsed = ResolveRequest();
        switch (parsed.status)
        {
            case ProtocolStatus::kIncomplete:
                return {.status = RequestStatus::kNeedMore};
            case ProtocolStatus::kProtocolError:
                ClearReadBuffer();
                return {.status = RequestStatus::kProtocolError, .error_message = std::move(parsed.error_message)};
            case ProtocolStatus::kOk:
                break;
        }

        const bool has_extra_data = ConsumeReadBuffer(parsed.consumed_bytes);
        if (has_extra_data)
        {
            // Pipelined requests are not supported yet: drain and reject so the
            // client gets a deterministic error instead of a desynchronised stream.
            ClearReadBuffer();
            return {.status = RequestStatus::kProtocolError,
                .error_message = "pipelined commands are not supported"};
        }

        ResolvedRequest resolved;
        resolved.status = RequestStatus::kReady;
        resolved.command = std::move(parsed.command);
        return resolved;
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
        const std::string encoded = parser_.SerializeResponse(result);
        WithWriteBuffer([&encoded](std::pmr::vector<char>& buffer) {
            buffer.assign(encoded.begin(), encoded.end());
        });
    }

    // Receive seam ----------------------------------------------------------
    // The networking model provides a destination for the next chunk of bytes
    // and, once the OS reports how many were read, hands the count back.

    template <typename F>
    auto WithReceiveContext(F&& f)
    {
        receive_offset_ = read_buffer_.size();
        read_buffer_.resize(receive_offset_ + receive_chunk_size_);
        return f(connection_.GetFileDescriptor(), read_buffer_.data() + receive_offset_, receive_chunk_size_);
    }

    void CompleteReceive(int result)
    {
        const auto received = result > 0 ? static_cast<std::size_t>(result) : 0;
        read_buffer_.resize(receive_offset_ + received);
    }

    // Send seam -------------------------------------------------------------
    // Mirror of the receive seam for flushing the prepared response.

    template <typename F>
    auto WithSendContext(F&& f) const
    {
        return f(connection_.GetFileDescriptor(), write_buffer_.data() + write_offset_, write_buffer_.size() - write_offset_);
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

    void Reset()
    {
        connection_.Reset();
        read_buffer_.clear();
        write_buffer_.clear();
        receive_offset_ = 0;
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
        write_buffer_(resource_)
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

    RequestParse ResolveRequest() const
    {
        return WithReadBuffer([this](const std::pmr::vector<char>& buffer) {
            return parser_.ParseRequest(std::string_view(buffer.data(), buffer.size()));
        });
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
        receive_offset_ = 0;
    }

    static constexpr std::size_t receive_chunk_size_ = 4096;
    const NetworkingModel networking_model_;
    std::pmr::memory_resource* resource_ = std::pmr::get_default_resource();
    Parser parser_;
    Connection connection_;
    std::pmr::vector<char> read_buffer_{resource_};
    std::pmr::vector<char> write_buffer_{resource_};
    std::size_t receive_offset_ = 0;
    std::size_t write_offset_ = 0;
};

// A pure virtual destructor still needs a definition, since derived
// destructors invoke it as part of their destruction sequence.
inline Session::~Session() = default;
} // namespace KV
