#include <resp/encoder.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstring>
#include <limits>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace resp {
namespace {
struct PromiseAccess {
    Encoder::promise_type* promise = nullptr;
    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<Encoder::promise_type> handle) noexcept {
        promise = &handle.promise();
        return false;
    }
    Encoder::promise_type& await_resume() const noexcept { return *promise; }
};

struct Frame {
    enum class Kind { kObject, kBytes, kValues, kPairs };
    Kind kind = Kind::kObject;
    const Object* object = nullptr;
    const std::vector<Object>* values = nullptr;
    const std::vector<std::pair<Object, Object>>* pairs = nullptr;
    std::string owned{};
    std::string_view borrowed{};
    std::size_t index = 0;
    std::size_t depth = 0;
    bool second = false;
};

void Bytes(std::vector<Frame>& frames, std::string_view bytes) {
    frames.push_back(Frame{.kind = Frame::Kind::kBytes, .borrowed = bytes});
}

void Owned(std::vector<Frame>& frames, std::string bytes) {
    frames.push_back(Frame{.kind = Frame::Kind::kBytes, .owned = std::move(bytes)});
}

void Line(std::vector<Frame>& frames, char marker, std::string_view text) {
    Bytes(frames, "\r\n");
    Bytes(frames, text);
    Owned(frames, std::string(1, marker));
}

void Length(std::vector<Frame>& frames, char marker, std::size_t size) {
    Owned(frames, std::string(1, marker) + std::to_string(size) + "\r\n");
}

bool Expand(std::vector<Frame>& frames, const Object& object, std::size_t depth) {
    if (depth > 128) return false;
    return std::visit([&frames, depth](const auto& value) {
        using Type = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<Type, SimpleString> || std::is_same_v<Type, SimpleError> ||
                      std::is_same_v<Type, BigNumber>) {
            if (value.value.find_first_of("\r\n") != std::string::npos) return false;
            if constexpr (std::is_same_v<Type, BigNumber>) {
                std::string_view digits = value.value;
                if (!digits.empty() && (digits.front() == '-' || digits.front() == '+')) digits.remove_prefix(1);
                if (digits.empty() || digits.find_first_not_of("0123456789") != std::string_view::npos) return false;
            }
            constexpr char marker = std::is_same_v<Type, SimpleString> ? '+' :
                                    (std::is_same_v<Type, SimpleError> ? '-' : '(');
            Line(frames, marker, value.value);
        } else if constexpr (std::is_same_v<Type, Integer>) {
            Owned(frames, ":" + std::to_string(value.value) + "\r\n");
        } else if constexpr (std::is_same_v<Type, Double>) {
            std::array<char, 64> text{};
            const auto [end, error] = std::to_chars(text.data(), text.data() + text.size(), value.value,
                                                  std::chars_format::general,
                                                  std::numeric_limits<double>::max_digits10);
            if (error != std::errc{}) return false;
            Owned(frames, "," + std::string(text.data(), end) + "\r\n");
        } else if constexpr (std::is_same_v<Type, Null>) {
            Bytes(frames, "_\r\n");
        } else if constexpr (std::is_same_v<Type, Boolean>) {
            Bytes(frames, value.value ? "#t\r\n" : "#f\r\n");
        } else if constexpr (std::is_same_v<Type, BulkString> || std::is_same_v<Type, BulkError>) {
            if constexpr (std::is_same_v<Type, BulkString>) {
                if (!value.value) {
                    Bytes(frames, "$-1\r\n");
                } else {
                    Bytes(frames, "\r\n");
                    Bytes(frames, *value.value);
                    Length(frames, '$', value.value->size());
                }
            } else {
                Bytes(frames, "\r\n");
                Bytes(frames, value.value);
                Length(frames, '!', value.value.size());
            }
        } else if constexpr (std::is_same_v<Type, VerbatimString>) {
            if (value.format.size() != 3) return false;
            Bytes(frames, "\r\n");
            Bytes(frames, value.value);
            Bytes(frames, ":");
            Bytes(frames, value.format);
            Length(frames, '=', value.value.size() + 4);
        } else if constexpr (std::is_same_v<Type, Array> || std::is_same_v<Type, Set> ||
                             std::is_same_v<Type, Push>) {
            frames.push_back(Frame{.kind = Frame::Kind::kValues, .values = &value.values, .depth = depth + 1});
            constexpr char marker = std::is_same_v<Type, Array> ? '*' : (std::is_same_v<Type, Set> ? '~' : '>');
            Length(frames, marker, value.values.size());
        } else {
            frames.push_back(Frame{.kind = Frame::Kind::kPairs, .pairs = &value.values, .depth = depth + 1});
            Length(frames, std::is_same_v<Type, Map> ? '%' : '|', value.values.size());
        }
        return true;
    }, object.value);
}
} // namespace

std::error_code make_error_code(EncodeError error) noexcept {
    static const EncodeErrorCategory category;
    return {static_cast<int>(error), category};
}

Encoder Encoder::promise_type::get_return_object() noexcept {
    return Encoder(std::coroutine_handle<promise_type>::from_promise(*this));
}

void Encoder::promise_type::unhandled_exception() noexcept {
    status = nbio::utility::unexpected{make_error_code(EncodeError::kInternal)};
    completed = true;
}

Encoder::Encoder(const Object& object) : Encoder(Generate(object)) {}
Encoder::Encoder(Encoder&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
Encoder& Encoder::operator=(Encoder&& other) noexcept {
    if (this != &other) {
        if (handle_) handle_.destroy();
        handle_ = std::exchange(other.handle_, {});
    }
    return *this;
}
Encoder::~Encoder() noexcept { if (handle_) handle_.destroy(); }

bool Encoder::done() const noexcept { return !handle_ || handle_.done() || handle_.promise().completed; }

EncodeResult Encoder::Encode(std::span<char> buffer) {
    if (!handle_) return {nbio::utility::unexpected{make_error_code(EncodeError::kInternal)}, 0};
    auto& promise = handle_.promise();
    if (done()) {
        if (!promise.status) return {nbio::utility::unexpected{promise.status.error()}, 0};
        return {buffer.first(0), 0};
    }
    promise.next = buffer;
    promise.encoded = 0;
    handle_.resume();
    return {promise.status, promise.encoded};
}

Encoder Encoder::Generate(const Object& object) {
    auto& promise = co_await PromiseAccess{}; // this stores the current promise and does not suspend
    std::vector<Frame> frames{{.object = &object}};
    std::size_t encoded = 0;
    auto output = promise.next;
    while (!frames.empty()) {
        auto& frame = frames.back();
        if (frame.kind == Frame::Kind::kObject) {
            const Object* current = frame.object;
            const std::size_t depth = frame.depth;
            frames.pop_back();
            if (!Expand(frames, *current, depth)) {
                co_yield EncodeResult{nbio::utility::unexpected{make_error_code(EncodeError::kInternal)}, encoded};
                co_return;
            }
        } else if (frame.kind == Frame::Kind::kValues || frame.kind == Frame::Kind::kPairs) {
            const std::size_t size = frame.values ? frame.values->size() : frame.pairs->size();
            if (frame.index == size) {
                frames.pop_back();
                continue;
            }
            const Object* child = nullptr;
            if (frame.values) child = &(*frame.values)[frame.index++];
            else {
                const auto& pair = (*frame.pairs)[frame.index];
                child = frame.second ? &pair.second : &pair.first;
                if (frame.second) ++frame.index;
                frame.second = !frame.second;
            }
            const std::size_t depth = frame.depth;
            frames.push_back(Frame{.object = child, .depth = depth});
        } else {
            const std::string_view bytes = frame.owned.empty() ? frame.borrowed : std::string_view(frame.owned);
            if (frame.index == bytes.size()) {
                frames.pop_back();
                continue;
            }
            if (promise.next.empty()) {
                co_yield EncodeResult{nbio::utility::unexpected{make_error_code(EncodeError::kMore)}, encoded};
                encoded = 0;
                output = promise.next;
                continue;
            }
            const std::size_t size = std::min(promise.next.size(), bytes.size() - frame.index);
            std::memcpy(promise.next.data(), bytes.data() + frame.index, size);
            frame.index += size;
            promise.next = promise.next.subspan(size);
            encoded += size;
            promise.encoded = encoded;
        }
    }
    co_yield EncodeResult{output.first(encoded), encoded};
}
}