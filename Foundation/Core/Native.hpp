#pragma once

namespace Foundation::Core
{
#if defined(_WIN32)
    using NativeHandle = void *;
#else
    using NativeHandle = int;
#endif
} // namespace Foundation::Core