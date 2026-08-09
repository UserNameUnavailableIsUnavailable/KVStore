#include "Common/MemoryResource.hpp"

#include <jemalloc/jemalloc.h>
#include <dlfcn.h>
#include <algorithm>
#include <cstddef>
#include <new>
#include <stdexcept>

namespace
{
class JemallocApi
{
public:
    using Allocate = void* (*)(std::size_t, int);
    using Deallocate = void (*)(void*, int);

    JemallocApi()
    {
        allocate = reinterpret_cast<Allocate>(::dlsym(RTLD_DEFAULT, "mallocx"));
        deallocate = reinterpret_cast<Deallocate>(::dlsym(RTLD_DEFAULT, "dallocx"));
        if (allocate == nullptr || deallocate == nullptr)
        {
            throw std::runtime_error("jemalloc must be loaded at process startup");
        }
    }

    Allocate allocate;
    Deallocate deallocate;
};

JemallocApi& GetJemallocApi()
{
    static JemallocApi* api = new JemallocApi();
    return *api;
}
} // namespace

namespace KV
{
void* JemallocMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment)
{
    void* ptr = GetJemallocApi().allocate(
        std::max(bytes, std::size_t(1)),
        MALLOCX_ALIGN(alignment)
    );
    if (ptr == nullptr)
    {
        throw std::bad_alloc();
    }
    return ptr;
}

void JemallocMemoryResource::do_deallocate(
    void* pointer,
    std::size_t,
    std::size_t)
{
    GetJemallocApi().deallocate(pointer, 0);
}

bool JemallocMemoryResource::do_is_equal(
    const std::pmr::memory_resource& other) const noexcept
{
    return this == &other;
}
} // namespace KV