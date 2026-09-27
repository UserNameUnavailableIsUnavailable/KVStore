#pragma once

#define CONTAINTER_OF(ptr, type, member) \
    ([]() { \
        static_assert(std::is_standard_layout_v<type>, \
                      "container_of requires a standard-layout type"); \
        return reinterpret_cast<type*>( \
            reinterpret_cast<char*>(ptr) - offsetof(type, member)); \
    }())