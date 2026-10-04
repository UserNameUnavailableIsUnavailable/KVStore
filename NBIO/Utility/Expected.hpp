#pragma once

#include <version>

#if defined(__cpp_lib_expected) && __cpp_lib_expected >= 202202L
#define HAS_STD_expected
#endif

#if defined(HAS_STD_expected)
#include <expected>
namespace NBIO::Utility {
using ::std::expected;
using ::std::unexpected;
}  // namespace NBIO::Utility
#else
#include <tl/expected.hpp>
namespace NBIO::Utility {
using ::tl::expected;
using ::tl::unexpected;
}  // namespace NBIO::Utility
#endif  // !defined(HAS_STD_expected)


