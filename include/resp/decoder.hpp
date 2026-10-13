#pragma once

#include <cassert>
#include <coroutine>
#include <cstddef>
#include <span>
#include <system_error>
#include <nbio/utility/expected.hpp>

#include "types.hpp"

namespace resp {

enum class DecodeError {
    kMore,
    kInvalid,
    kInternal,
};

class DecodeErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "Decoder";
    }

    std::string message(int ev) const override {
        switch (static_cast<DecodeError>(ev)) {
            case DecodeError::kMore:
                return "more data needed";
            case DecodeError::kInvalid:
                return "invalid data";
            case DecodeError::kInternal:
                return "internal error";
            default:
                assert(false && "Unknown decode error");
                return "unknown error";
        }
    }
};

struct DecodeResult {
    nbio::utility::expected<Object, DecodeError> value;
    std::size_t decoded = 0;
};

class Decoder {
public:
    struct promise_type {
        nbio::utility::expected<Object, DecodeError> value{nbio::utility::unexpected{DecodeError::kMore}};
        std::size_t decoded = 0;
        std::span<const char> next;
        Decoder get_return_object() noexcept;
        std::suspend_always initial_suspend() const noexcept { return {}; }
        std::suspend_always final_suspend() const noexcept { return {}; }
        std::suspend_always yield_value(DecodeResult result) noexcept;
        void return_void() const noexcept {}
        void unhandled_exception() noexcept;
    };

    Decoder();

    Decoder(const Decoder&) = delete;
    Decoder& operator=(const Decoder&) = delete;
    Decoder(Decoder&& other) noexcept;
    Decoder& operator=(Decoder&& other) noexcept;
    ~Decoder() noexcept;

    DecodeResult Decode(std::span<const char> buffer);

private:
    explicit Decoder(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle) {}
    static Decoder Generate();
    std::coroutine_handle<promise_type> handle_{};
};
} // namespace resp