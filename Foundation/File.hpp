#pragma once

namespace Foundation
{
class FileMapView
{
public:
#if defined(__linux__)
    using Handle = int;
    static constexpr Handle kInvalidHandle{ -1 };
#endif
private:
    Handle handle_{ -1 };
};
} // namespace Foundation