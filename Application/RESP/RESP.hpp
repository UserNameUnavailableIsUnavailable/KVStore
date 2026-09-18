#pragma once

#include <Foundation/Async/Async.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <coroutine>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace RESP
{
struct SimpleString
{
    std::string value;
};

struct SimpleError
{
    std::string value;
};

struct Integer
{
    std::int64_t value;
};
struct BulkString
{
    std::optional<std::string> value;
};
struct Null
{
};
struct Boolean
{
    bool value;
};
struct Double
{
    double value;
};
struct BigNumber
{
    std::string value;
};
struct BulkError
{
    std::string value;
};
struct VerbatimString
{
    std::string format;
    std::string value;
};

struct Object;
struct Array
{
    std::vector<Object> values;
};
struct Set
{
    std::vector<Object> values;
};
struct Map
{
    std::vector<std::pair<Object, Object>> values;
};
struct Attribute
{
    std::vector<std::pair<Object, Object>> values;
};
struct Push
{
    std::vector<Object> values;
};

struct Object
{
    using Value = std::variant<SimpleString, SimpleError, Integer, BulkString, Null, Boolean, Double, BigNumber,
                               BulkError, VerbatimString, Array, Set, Map, Attribute, Push>;

    Value value;

    // Object MUST be explicitly constructed.
    // Sender & Receiver may hold references to objects, implicit construction can cause dangling references.
    template <typename T>
    Object(T &&object) : value(std::forward<T>(object))
    {
    }
};

enum class DecodeStatus
{
    kNeedInput,
    kComplete,
    kProtocolError,
};

struct DecodeResult
{
    DecodeStatus status = DecodeStatus::kNeedInput;
    std::optional<Object> object;
    std::string error;
};

enum class EncodeStatus
{
    kComplete,
    kNeedFlush,
};

class Encoder
{
  public:
    struct promise_type
    {
        Encoder get_return_object() noexcept;
        std::suspend_always initial_suspend() noexcept;
        std::suspend_always final_suspend() noexcept;
        std::suspend_always yield_value(EncodeStatus status) noexcept;
        void return_void() noexcept;
        void unhandled_exception() noexcept;

        EncodeStatus status_ = EncodeStatus::kNeedFlush;
    };

    Encoder() noexcept = default;
    explicit Encoder(std::coroutine_handle<promise_type> handle) noexcept;
    Encoder(const Encoder &) = delete;
    Encoder &operator=(const Encoder &) = delete;
    Encoder(Encoder &&other) noexcept;
    Encoder &operator=(Encoder &&other) noexcept;
    ~Encoder() noexcept;

    [[nodiscard]] bool done() const noexcept;
    // Advances until output must be flushed or encoding is complete.
    EncodeStatus poll() noexcept;
    void resume() noexcept;
    [[nodiscard]] EncodeStatus status() const noexcept;

  private:
    std::coroutine_handle<promise_type> handle_{};
};

class Decoder
{
  public:
    struct promise_type
    {
        Decoder get_return_object() noexcept;
        std::suspend_always initial_suspend() noexcept;
        std::suspend_always final_suspend() noexcept;
        std::suspend_always yield_value(DecodeStatus status) noexcept;
        void return_value(DecodeResult result) noexcept;
        void unhandled_exception() noexcept;

        DecodeStatus status_ = DecodeStatus::kNeedInput;
        DecodeResult result_;
    };

    Decoder() noexcept = default;
    explicit Decoder(std::coroutine_handle<promise_type> handle) noexcept;
    Decoder(const Decoder &) = delete;
    Decoder &operator=(const Decoder &) = delete;
    Decoder(Decoder &&other) noexcept;
    Decoder &operator=(Decoder &&other) noexcept;
    ~Decoder() noexcept;

    [[nodiscard]] bool done() const noexcept;
    // Advances until more input is required, decoding completes, or a protocol
    // error is found.
    DecodeStatus poll();
    void resume();
    [[nodiscard]] DecodeStatus status() const noexcept;
    [[nodiscard]] const DecodeResult &result() const noexcept;
    // The same result, movable: a decoder that has finished with it hands the
    // object over instead of copying it.
    [[nodiscard]] DecodeResult &result() noexcept;

  private:
    std::coroutine_handle<promise_type> handle_{};
};

// The two dialects of the protocol. A client may write either: redis-cli and
// redis-benchmark's PING_INLINE test send a line of words rather than a
// multi-bulk. The replication link is written by this program, which only ever
// sends multi-bulk, so a word where a type marker belongs stays the protocol
// error it is there.
enum class Dialect
{
    kMultibulkOnly,
    kMultibulkAndInline,
};

enum class ScanStatus
{
    // Not all of the command is here yet: read more and scan the same bytes
    // again.
    kNeedInput,
    // One command, complete.
    kComplete,
    // Not a command in the shape this reads: an inline line, an argument that is
    // not a bulk string, a number too long to be a length, a payload that is not
    // followed by CRLF. Nothing has been taken out of the buffer, so the decoder
    // can be asked for the same bytes instead -- it reads every shape of the
    // protocol, and it is the one that says what is wrong with this one.
    kNotACommand,
};

// Reads one command out of `buffer` where it lies, without taking it out of the
// buffer: `words` is filled with the arguments as views into the bytes and `size`
// with how many bytes of the buffer the command occupies, and the caller
// consumes those bytes once it is done with the words.
//
// This is the shape a client writes a command in -- a RESP array of bulk strings
// -- read in one pass with nothing copied, which is what a server answering a
// million commands a second needs. Nothing is consumed until the caller says so,
// which is what lets a command that is only half here be scanned again from the
// start when the rest of it arrives: the scanner holds no state and remembers
// nothing between calls.
//
// `words` is the caller's, so a connection can scan a million commands through
// one vector and allocate for none of them.
ScanStatus ScanCommand(const ::Foundation::Core::Buffer &buffer, std::vector<std::string_view> &words, std::size_t &size);

Decoder Decode(::Foundation::Core::Buffer &buffer, Dialect dialect = Dialect::kMultibulkOnly);
Encoder Encode(const Object &object, ::Foundation::Core::Buffer &buffer);
Encoder Encode(Object &&object, ::Foundation::Core::Buffer &buffer) = delete;
// The bytes of `object`, written into `buffer` without building anything on the
// way: no string to spell out a number, no list of pieces for the reply to be
// copied out of, no frame for a coroutine to suspend in. A reply is not a
// document to assemble, it is bytes to put where they are going.
//
// The buffer grows to hold what does not fit, so the caller needs nowhere to
// flush to; false means the buffer refused to grow any further, which is a reply
// larger than its ceiling allows.
//
// `Encode` says the same thing as a coroutine, and is what a writer that has to
// flush in the middle of a reply -- the AOF spilling to the file -- uses. This is
// for the writer that does not: the reply batch, which is answered to a client
// and has no other way out.
bool AppendObject(const Object &object, ::Foundation::Core::Buffer &buffer);
} // namespace RESP
