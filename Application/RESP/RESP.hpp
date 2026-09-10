#pragma once

#include <Foundation/Async/Async.hpp>
#include <Foundation/Buffer.hpp>
#include <coroutine>
#include <cstdint>
#include <optional>
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

  private:
    std::coroutine_handle<promise_type> handle_{};
};

Decoder Decode(::Foundation::Buffer &buffer);
Encoder Encode(const Object &object, ::Foundation::Buffer &buffer);
Encoder Encode(Object &&object, ::Foundation::Buffer &buffer) = delete;
} // namespace RESP
