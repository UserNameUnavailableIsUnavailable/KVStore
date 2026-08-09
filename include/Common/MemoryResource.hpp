#pragma once

#include <memory_resource>

namespace KV
{
class JemallocMemoryResource final : public std::pmr::memory_resource
{
private:
    [[nodiscard]] virtual void* do_allocate(std::size_t bytes, std::size_t alignment) override;
    virtual void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;
    [[nodiscard]] virtual bool do_is_equal(const memory_resource& other) const noexcept override;
};
} // namespace KV