#include <resp/decoder.hpp>

#include <algorithm>
#include <charconv>
#include <limits>
#include <string_view>
#include <utility>

namespace resp {
namespace {
struct PromiseAccess {
    Decoder::promise_type* promise = nullptr;
    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<Decoder::promise_type> handle) noexcept {
        promise = &handle.promise();
        return false;
    }
    Decoder::promise_type& await_resume() const noexcept { return *promise; }
};

struct Aggregate {
    char marker;
    std::size_t remaining;
    std::vector<Object> values{};
    std::vector<std::pair<Object, Object>> pairs{};
    std::optional<Object> key{};

    Object Finish() {
        switch (marker) {
            case '*': return Object(Array{std::move(values)});
            case '~': return Object(Set{std::move(values)});
            case '>': return Object(Push{std::move(values)});
            case '%': return Object(Map{std::move(pairs)});
            default: return Object(Attribute{std::move(pairs)});
        }
    }
};

struct Header {
    std::optional<Object> object;
    std::size_t size = 0;
    bool bulk = false;
};

bool IntegerValue(std::string_view text, std::int64_t& value) {
    if (!text.empty() && text.front() == '+') {
        text.remove_prefix(1);
        if (text.empty() || text.front() == '-') return false;
    }
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    return error == std::errc{} && end == text.data() + text.size();
}

std::optional<Header> ParseHeader(char marker, std::string line) {
    Header header;
    if (marker == '+') header.object.emplace(SimpleString{std::move(line)});
    else if (marker == '-') header.object.emplace(SimpleError{std::move(line)});
    else if (marker == '(') {
        std::string_view digits = line;
        if (!digits.empty() && (digits.front() == '+' || digits.front() == '-')) digits.remove_prefix(1);
        if (digits.empty() || digits.find_first_not_of("0123456789") != std::string_view::npos) return std::nullopt;
        header.object.emplace(BigNumber{std::move(line)});
    } else if (marker == '_') {
        if (!line.empty()) return std::nullopt;
        header.object.emplace(Null{});
    } else if (marker == '#') {
        if (line != "t" && line != "f") return std::nullopt;
        header.object.emplace(Boolean{line == "t"});
    } else if (marker == ':') {
        std::int64_t value = 0;
        if (!IntegerValue(line, value)) return std::nullopt;
        header.object.emplace(Integer{value});
    } else if (marker == ',') {
        double value = 0;
        std::string_view text = line;
        if (!text.empty() && text.front() == '+') text.remove_prefix(1);
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
        if (error != std::errc{} || end != text.data() + text.size()) return std::nullopt;
        header.object.emplace(Double{value});
    } else {
        std::int64_t size = 0;
        if (!IntegerValue(line, size)) return std::nullopt;
        if (size == -1 && (marker == '$' || marker == '*')) {
            if (marker == '$') header.object.emplace(BulkString{std::nullopt});
            else header.object.emplace(Null{});
        } else {
            if (size < 0 || static_cast<std::uint64_t>(size) > std::numeric_limits<std::size_t>::max())
                return std::nullopt;
            header.size = static_cast<std::size_t>(size);
            header.bulk = marker == '$' || marker == '!' || marker == '=';
            if (marker == '=' && header.size < 4) return std::nullopt;
        }
    }
    return header;
}
} // namespace

Decoder Decoder::promise_type::get_return_object() noexcept {
    return Decoder(std::coroutine_handle<promise_type>::from_promise(*this));
}
std::suspend_always Decoder::promise_type::yield_value(DecodeResult result) noexcept {
    value = std::move(result.value);
    decoded = result.decoded;
    return {};
}
void Decoder::promise_type::unhandled_exception() noexcept {
    value = nbio::utility::unexpected{DecodeError::kInternal};
}
Decoder::Decoder() : Decoder(Generate()) {}
Decoder::Decoder(Decoder&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
Decoder& Decoder::operator=(Decoder&& other) noexcept {
    if (this != &other) {
        if (handle_) handle_.destroy();
        handle_ = std::exchange(other.handle_, {});
    }
    return *this;
}
Decoder::~Decoder() noexcept { if (handle_) handle_.destroy(); }

DecodeResult Decoder::Decode(std::span<const char> buffer) {
    if (!handle_) return {nbio::utility::unexpected{DecodeError::kInternal}, 0};
    auto& promise = handle_.promise();
    if (handle_.done() || (!promise.value && promise.value.error() != DecodeError::kMore)) {
        return {nbio::utility::unexpected{promise.value.error()}, 0};
    }
    promise.next = buffer;
    promise.decoded = 0;
    handle_.resume();
    return {std::move(promise.value), promise.decoded};
}

Decoder Decoder::Generate() {
    enum class State { kMarker, kLine, kPayload, kTrailer };
    auto& promise = co_await PromiseAccess{};
    State state = State::kMarker;
    std::vector<Aggregate> aggregates;
    std::string line;
    std::string payload;
    char marker = 0;
    bool carriage_return = false;
    std::size_t remaining = 0;
    std::size_t trailer = 0;
    std::size_t decoded = 0;
    while (true) {
        if (promise.next.empty()) {
            co_yield DecodeResult{nbio::utility::unexpected{DecodeError::kMore}, decoded};
            decoded = 0;
            continue;
        }
        std::optional<Object> object;
        bool invalid = false;
        if (state == State::kPayload) {
            const std::size_t size = std::min(remaining, promise.next.size());
            payload.append(promise.next.data(), size);
            promise.next = promise.next.subspan(size);
            decoded += size;
            remaining -= size;
            if (remaining == 0) state = State::kTrailer;
        } else {
            const char character = promise.next.front();
            promise.next = promise.next.subspan(1);
            ++decoded;
            if (state == State::kMarker) {
                marker = character;
                invalid = std::string_view("+-:$*_#,(!=~%|>").find(marker) == std::string_view::npos;
                line.clear();
                carriage_return = false;
                state = State::kLine;
            } else if (state == State::kLine) {
                if (carriage_return) {
                    invalid = character != '\n';
                    if (!invalid) {
                        auto header = ParseHeader(marker, std::move(line));
                        invalid = !header;
                        if (header) {
                            if (header->object) {
                                object = std::move(header->object);
                                state = State::kMarker;
                            } else if (header->bulk) {
                                remaining = header->size;
                                payload.clear();
                                trailer = 0;
                                state = remaining == 0 ? State::kTrailer : State::kPayload;
                            } else {
                                invalid = aggregates.size() >= 128;
                                if (!invalid) {
                                    Aggregate aggregate{.marker = marker, .remaining = header->size};
                                    if (aggregate.remaining == 0) object = aggregate.Finish();
                                    else aggregates.push_back(std::move(aggregate));
                                    state = State::kMarker;
                                }
                            }
                        }
                    }
                } else if (character == '\r') carriage_return = true;
                else if (character == '\n') invalid = true;
                else line.push_back(character);
            } else {
                invalid = character != (trailer == 0 ? '\r' : '\n');
                if (!invalid && ++trailer == 2) {
                    if (marker == '$') object.emplace(BulkString{std::move(payload)});
                    else if (marker == '!') object.emplace(BulkError{std::move(payload)});
                    else {
                        invalid = payload.size() < 4 || payload[3] != ':';
                        if (!invalid) object.emplace(VerbatimString{payload.substr(0, 3), payload.substr(4)});
                    }
                    state = State::kMarker;
                }
            }
        }
        promise.decoded = decoded;
        if (invalid) {
            co_yield DecodeResult{nbio::utility::unexpected{DecodeError::kInvalid}, decoded};
            co_return;
        }
        while (object && !aggregates.empty()) {
            auto& aggregate = aggregates.back();
            if (aggregate.marker == '%' || aggregate.marker == '|') {
                if (!aggregate.key) {
                    aggregate.key = std::move(object);
                    object.reset();
                    break;
                }
                aggregate.pairs.emplace_back(std::move(*aggregate.key), std::move(*object));
                aggregate.key.reset();
            } else aggregate.values.push_back(std::move(*object));
            object.reset();
            if (--aggregate.remaining == 0) {
                object = aggregate.Finish();
                aggregates.pop_back();
            }
        }
        if (object) {
            co_yield DecodeResult{std::move(*object), decoded};
            decoded = 0;
        }
    }
}
} // namespace resp