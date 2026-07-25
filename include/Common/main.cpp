#include "Common/LRUCache.hpp"

int main(int argc, char* argv[])
{
	KV::LRUCache<std::string> lru(2);
	lru.Set("Foo", "bar");
	return 0;
}
