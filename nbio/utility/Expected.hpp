#pragma once

#include <version>

#if defined(__cpp_lib_expected) && __cpp_lib_expected >= 202202L
#define HAS_STD_expected
#endif

#if defined(HAS_STD_expected)
#include <expected>
namespace nbio::utility {
using ::std::expected;
using ::std::unexpected;
}  // namespace nbio::utility
#else
#include <tl/expected.hpp>
namespace nbio::utility {
using ::tl::expected;
using ::tl::unexpected;
}  // namespace nbio::utility
#endif  // !defined(HAS_STD_expected)


