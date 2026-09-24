#pragma once

#include <version>

#if defined(__cpp_lib_expected) && __cpp_lib_expected >= 202202L
#define HAS_STD_expected
#endif

#if defined(HAS_STD_expected)
#include <expected>
namespace Foundation::Core
{
using ::std::expected;
using ::std::unexpected;
} // namespace Foundation::Core
#else
#include <tl/expected.hpp>
namespace Foundation::Core
{
using ::tl::expected;
using ::tl::unexpected;
} // namespace Foundation::Core
#endif // !defined(HAS_STD_expected)
