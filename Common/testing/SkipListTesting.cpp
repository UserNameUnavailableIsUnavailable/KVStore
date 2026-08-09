#include "Common/SkipList.hpp"

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
    for (auto it = skip_list.Begin(); it != skip_list.End(); ++it)
    {
        keys.push_back((*it).first);
    }
    return keys;
}
} // namespace

TEST(SkipListTesting, InsertsEntriesInKeyOrder)
{
    SkipList skip_list;
    skip_list.Insert(30, "thirty");
    skip_list.Insert(10, "ten");
    skip_list.Insert(20, "twenty");

    EXPECT_FALSE(skip_list.IsEmpty());
    EXPECT_EQ(Keys(skip_list), (std::vector<int>{10, 20, 30}));
}

TEST(SkipListTesting, InsertWithExistingKeyUpdatesValue)
{
    SingleLevelSkipList skip_list;
    skip_list.Insert(10, "original");

    const auto inserted = skip_list.Insert(10, "updated");

    EXPECT_EQ((*inserted).first, 10);
    EXPECT_EQ((*inserted).second, "updated");
    EXPECT_EQ((*skip_list.Begin()).second, "updated");
    EXPECT_EQ(++skip_list.Begin(), skip_list.End());
}

TEST(SkipListTesting, FindsPresentKeyAndRejectsMissingKey)
{
    SingleLevelSkipList skip_list;
    skip_list.Insert(10, "ten");
    skip_list.Insert(20, "twenty");

    const auto found = skip_list.Find(20);

    ASSERT_NE(found, skip_list.End());
    EXPECT_EQ((*found).first, 20);
    EXPECT_EQ((*found).second, "twenty");
    EXPECT_EQ(skip_list.Find(15), skip_list.End());
}

TEST(SkipListTesting, ErasesFirstMiddleLastAndOnlyNode)
{
    SingleLevelSkipList skip_list;
    skip_list.Insert(10, "ten");
    skip_list.Insert(20, "twenty");
    skip_list.Insert(30, "thirty");

    EXPECT_EQ(skip_list.Erase(10), 1U);
    EXPECT_EQ(Keys(skip_list), (std::vector<int>{20, 30}));
    EXPECT_EQ(skip_list.Erase(20), 1U);
    EXPECT_EQ(Keys(skip_list), (std::vector<int>{30}));
    EXPECT_EQ(skip_list.Erase(30), 1U);
    EXPECT_TRUE(skip_list.IsEmpty());
    EXPECT_EQ(skip_list.Erase(30), 0U);
}

TEST(SkipListTesting, ErasesMultipleKeysFromDefaultLevelConfiguration)
{
    SkipList skip_list;
    for (int key = 0; key < 100; ++key)
    {
        skip_list.Insert(key, "value");
    }

    for (int key = 0; key < 100; key += 2)
    {
        EXPECT_EQ(skip_list.Erase(key), 1U);
    }

    std::vector<int> expected_keys;
    for (int key = 1; key < 100; key += 2)
    {
        expected_keys.push_back(key);
    }
    EXPECT_EQ(Keys(skip_list), expected_keys);

    for (int key = 1; key < 100; key += 2)
    {
        EXPECT_EQ(skip_list.Erase(key), 1U);
    }
    EXPECT_TRUE(skip_list.IsEmpty());
}