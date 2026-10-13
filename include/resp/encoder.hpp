#pragma once

#include <cassert>
#include <coroutine>
#include <cstddef>
#include <span>
#include <system_error>
#include <nbio/utility/expected.hpp>

#include "types.hpp"

namespace resp {
enum class EncodeError {
    kMore,
    kInternal,
};

class EncodeErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "Encoder";
    }

    std::string message(int ev) const override {
        switch (static_cast<EncodeError>(ev)) {
            case EncodeError::kMore:
                return "more data needed";
            case EncodeError::kInternal:
                return "internal error";
            default:
                assert(false && "Unknown encode error");
                return "unknown error";
        }
    }
};

struct EncodeResult {
    nbio::utility::expected<std::span<char>, std::error_code> status;
    std::size_t encoded = 0;
};

std::error_code make_error_code(EncodeError error) noexcept;

class Encoder {
public:
    struct promise_type {
        nbio::utility::expected<std::span<char>, std::error_code> status{std::span<char>{}};
        std::size_t encoded = 0;
        std::span<char> next;
        bool completed = false;
        Encoder get_return_object() noexcept;
        std::suspend_always yield_value(EncodeResult result) noexcept {
            status = result.status;
            encoded = result.encoded;
            completed = status.has_value() || status.error() != make_error_code(EncodeError::kMore);
            return {};
        }
        std::suspend_always initial_suspend() const noexcept { return {}; }
        std::suspend_always final_suspend() const noexcept { return {}; }
        void return_void() const noexcept {}
        void unhandled_exception() noexcept;
    };

    Encoder(const Object& object);

    Encoder(Object&&) = delete;
    Encoder(const Encoder&) = delete;
    Encoder& operator=(const Encoder&) = delete;
    Encoder(Encoder&& other) noexcept;
    Encoder& operator=(Encoder&& other) noexcept;

    ~Encoder() noexcept;

    EncodeResult Encode(std::span<char> buffer);
    bool done() const noexcept;

private:
    explicit Encoder(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle) {}
    static Encoder Generate(const Object& object);
    std::coroutine_handle<promise_type> handle_{};
};
} // namespace resp