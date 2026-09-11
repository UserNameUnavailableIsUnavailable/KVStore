#pragma once

namespace Foundation
{
#if defined(_WIN32)
    using NativeHandle = void *;
#else
    using NativeHandle = int;
#endif
} // namespace Foundation