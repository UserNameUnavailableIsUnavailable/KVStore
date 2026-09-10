#include "SkipList.hpp"

#include <gtest/gtest.h>
#include <string_view>
#include <vector>

namespace
{
using SkipList = KV::SkipListMap<int, std::string_view>;
using SingleLevelSkipList = KV::SkipListMap<int, std::string_view, 1>;

template <typename List>
std::vector<int> Keys(List& skip_list)
{
    std::vector<int> keys;
    for (auto it = skip_list.begin(); it != skip_list.end(); ++it)
    {
        keys.push_back((*it).first);
    }
    return keys;
}
} // namespace

TEST(SkipListTesting, insertsEntriesInKeyOrder)
{
    SkipList skip_list;
    skip_list.insert(30, "thirty");
    skip_list.insert(10, "ten");
    skip_list.insert(20, "twenty");

    EXPECT_FALSE(skip_list.IsEmpty());
    EXPECT_EQ(Keys(skip_list), (std::vector<int>{10, 20, 30}));
}

TEST(SkipListTesting, insertWithExistingKeyUpdatesValue)
{
    SingleLevelSkipList skip_list;
    skip_list.insert(10, "original");

    const auto inserted = skip_list.insert(10, "updated");

    EXPECT_EQ((*inserted).first, 10);
    EXPECT_EQ((*inserted).second, "updated");
    EXPECT_EQ((*skip_list.begin()).second, "updated");
    EXPECT_EQ(++skip_list.begin(), skip_list.end());
}

TEST(SkipListTesting, findsPresentKeyAndRejectsMissingKey)
{
    SingleLevelSkipList skip_list;
    skip_list.insert(10, "ten");
    skip_list.insert(20, "twenty");

    const auto found = skip_list.find(20);

    ASSERT_NE(found, skip_list.end());
    EXPECT_EQ((*found).first, 20);
    EXPECT_EQ((*found).second, "twenty");
    EXPECT_EQ(skip_list.find(15), skip_list.end());
}

TEST(SkipListTesting, erasesFirstMiddleLastAndOnlyNode)
{
    SingleLevelSkipList skip_list;
    skip_list.insert(10, "ten");
    skip_list.insert(20, "twenty");
    skip_list.insert(30, "thirty");

    EXPECT_EQ(skip_list.erase(10), 1U);
    EXPECT_EQ(Keys(skip_list), (std::vector<int>{20, 30}));
    EXPECT_EQ(skip_list.erase(20), 1U);
    EXPECT_EQ(Keys(skip_list), (std::vector<int>{30}));
    EXPECT_EQ(skip_list.erase(30), 1U);
    EXPECT_TRUE(skip_list.IsEmpty());
    EXPECT_EQ(skip_list.erase(30), 0U);
}

TEST(SkipListTesting, erasesMultipleKeysFromDefaultLevelConfiguration)
{
    SkipList skip_list;
    for (int key = 0; key < 100; ++key)
    {
        skip_list.insert(key, "value");
    }

    for (int key = 0; key < 100; key += 2)
    {
        EXPECT_EQ(skip_list.erase(key), 1U);
    }

    std::vector<int> expected_keys;
    for (int key = 1; key < 100; key += 2)
    {
        expected_keys.push_back(key);
    }
    EXPECT_EQ(Keys(skip_list), expected_keys);

    for (int key = 1; key < 100; key += 2)
    {
        EXPECT_EQ(skip_list.erase(key), 1U);
    }
    EXPECT_TRUE(skip_list.IsEmpty());
}
