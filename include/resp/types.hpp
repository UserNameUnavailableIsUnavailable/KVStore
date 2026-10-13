#pragma once

#include <string>
#include <optional>
#include <cstdint>
#include <vector>
#include <variant>
#include <utility>

namespace resp {
struct SimpleString {
    std::string value;
};

struct SimpleError {
    std::string value;
};

struct Integer {
    std::int64_t value;
};
struct BulkString {
    std::optional<std::string> value;
};
struct Null {};

struct Boolean {
    bool value;
};
struct Double {
    double value;
};
struct BigNumber {
    std::string value;
};
struct BulkError {
    std::string value;
};
struct VerbatimString {
    std::string format;
    std::string value;
};

struct Object;
struct Array {
    std::vector<Object> values;
};
struct Set {
    std::vector<Object> values;
};
struct Map {
    std::vector<std::pair<Object, Object>> values;
};
struct Attribute {
    std::vector<std::pair<Object, Object>> values;
};
struct Push {
    std::vector<Object> values;
};

struct Object {
    using Value = std::variant<SimpleString, SimpleError, Integer, BulkString, Null, Boolean, Double, BigNumber,
                               BulkError, VerbatimString, Array, Set, Map, Attribute, Push>;

    Value value;

    // Object MUST be explicitly constructed.
    // Sender & Receiver may hold references to objects, implicit construction can cause dangling references.
    template <typename T>
    Object(T&& object) : value(std::forward<T>(object)) {}
};

} // namespace resp