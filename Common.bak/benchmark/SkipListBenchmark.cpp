#include <benchmark/benchmark.h>

#include <cstddef>
#include <cstdint>
#include <memory_resource>

#include "SkipListMap.hpp"

namespace
{
using SkipList = KV::SkipListMap<int, std::uint64_t>;

constexpr int MinElementCount = 1 << 10;
constexpr int MaxElementCount = 1 << 16;

void runinserteraseChurn(benchmark::State& state, std::pmr::memory_resource* resource)
{
	const int element_count = static_cast<int>(state.range(0));

	for (auto _ : state)
	{
		SkipList skip_list(resource);
		for (int key = 0; key < element_count; ++key)
		{
			skip_list.insert(key, static_cast<std::uint64_t>(key));
		}
		for (int key = 0; key < element_count; ++key)
		{
			benchmark::DoNotOptimize(skip_list.erase(key));
		}
		benchmark::DoNotOptimize(skip_list.IsEmpty());
	}

	state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(element_count) * 2);
}

void BM_DefaultResource(benchmark::State& state)
{
	runinserteraseChurn(state, std::pmr::new_delete_resource());
}

void BM_UnsynchronizedPoolResource(benchmark::State& state)
{
	std::pmr::unsynchronized_pool_resource pool;
	runinserteraseChurn(state, &pool);
}
}

BENCHMARK(BM_DefaultResource)->Range(MinElementCount, MaxElementCount);
BENCHMARK(BM_UnsynchronizedPoolResource)->Range(MinElementCount, MaxElementCount);

BENCHMARK_MAIN();
