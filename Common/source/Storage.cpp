#include "Common/Storage.hpp"

#include <stdexcept>

namespace KV
{
namespace
{
// Pick the eviction policy for an already-chosen storage structure.
template <template <typename, typename> class Container>
std::unique_ptr<Storage<std::string, std::string>> MakeWithEviction(
    EvictionStrategy eviction,
    std::size_t capacity,
    std::pmr::memory_resource* resource)
{
    switch (eviction)
    {
        case EvictionStrategy::kLru:
            return std::make_unique<CacheStorage<std::string, std::string, Container, LruPolicy>>(
                capacity, resource);
        case EvictionStrategy::kLfu:
            throw std::invalid_argument("LFU eviction is not implemented yet");
    }
    throw std::invalid_argument("unknown eviction strategy");
}
} // namespace

std::unique_ptr<Storage<std::string, std::string>> MakeStorage(
    StorageStructure structure,
    EvictionStrategy eviction,
    std::size_t capacity,
    std::pmr::memory_resource* resource)
{
    switch (structure)
    {
        case StorageStructure::kHash:
            return MakeWithEviction<HashContainer>(eviction, capacity, resource);
        case StorageStructure::kTree:
            return MakeWithEviction<TreeContainer>(eviction, capacity, resource);
        case StorageStructure::kArray:
            return MakeWithEviction<ArrayContainer>(eviction, capacity, resource);
    }
    throw std::invalid_argument("unknown storage structure");
}
} // namespace KV
