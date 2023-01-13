/**
* lru_k_replacer_test.cpp
*/

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, SampleTest) {
 LRUKReplacer lru_replacer(7, 2);

 // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
 lru_replacer.RecordAccess(1);
 lru_replacer.RecordAccess(2);
 lru_replacer.RecordAccess(3);
 lru_replacer.RecordAccess(4);
 lru_replacer.RecordAccess(5);
 lru_replacer.RecordAccess(6);
 lru_replacer.SetEvictable(1, true);
 lru_replacer.SetEvictable(2, true);
 lru_replacer.SetEvictable(3, true);
 lru_replacer.SetEvictable(4, true);
 lru_replacer.SetEvictable(5, true);
 lru_replacer.SetEvictable(6, false);
 ASSERT_EQ(5, lru_replacer.Size());

 // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
 // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
 lru_replacer.RecordAccess(1);

 // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
 // first based on LRU.

 // head: <6,1,f>, <5,1,t>, <4,1,t>, <3,1,t>, <2,1,t>, <1,1,t>
 // head: <6,1,f>, <5,1,t>, <4,1,t>, <3,1,t>, <2,1,t>  | head: <1,2,t>
 // evict
 // head: <6,1,f>, <5,1,t>, <4,1,t>, <3,1,t> | head: <1,2,t>
 // evict
 // head: <6,1,f>, <5,1,t>, <4,1,t> | head: <1,2,t>
 // evict
 // head: <6,1,f>, <5,1,t>  | head: <1,2,t>

 int value;
 lru_replacer.Evict(&value);
 ASSERT_EQ(2, value);
 lru_replacer.Evict(&value);
 ASSERT_EQ(3, value);
 lru_replacer.Evict(&value);
 ASSERT_EQ(4, value);
 ASSERT_EQ(2, lru_replacer.Size());

 // RecordAccess 3
 // head: <3,1,f>, <6,1,f>, <5,1,t>  | head: <1,2,t>
 // RecordAccess 4
 // head: <4,1,f>, <3,1,f>, <6,1,f>, <5,1,t>  | head: <1,2,t>
 // RecordAccess 5
 // head: <4,1,f>, <3,1,f>, <6,1,f>, | head: <5,2,t>, <1,2,t>
 // RecordAccess 4
 // head: <3,1,f>, <6,1,f> | head: <4,2,f>, <5,2,t>, <1,2,t>
 // set evicetable
 // head: <3,1,t>, <6,1,f> | head: <4,2,t>, <5,2,t>, <1,2,t>
 // evict
 // head: <6,1,f> | head: <4,2,t>, <5,2,t>, <1,2,t>
 // evict
 // head: | head: <4,2,t>, <5,2,t>, <1,2,t>
 // set evict false
 // head: | head: <4,2,t>, <5,2,t>, <1,2,f>
 // evict
 // head: | head: <4,2,t>, <1,2,f>

 // Scenario: Now replacer has frames [5,1].
 // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
 lru_replacer.RecordAccess(3);
 lru_replacer.RecordAccess(4);
 lru_replacer.RecordAccess(5);
 lru_replacer.RecordAccess(4);
 lru_replacer.SetEvictable(3, true);
 lru_replacer.SetEvictable(4, true);
 ASSERT_EQ(4, lru_replacer.Size());

 // Scenario: continue looking for victims. We expect 3 to be evicted next.
 lru_replacer.Evict(&value);
 ASSERT_EQ(3, value);
 ASSERT_EQ(3, lru_replacer.Size());

 // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
 lru_replacer.SetEvictable(6, true);
 ASSERT_EQ(4, lru_replacer.Size());
 lru_replacer.Evict(&value);
 ASSERT_EQ(6, value);
 ASSERT_EQ(3, lru_replacer.Size());

 // Now we have [1,5,4]. Continue looking for victims.
 lru_replacer.SetEvictable(1, false);
 ASSERT_EQ(2, lru_replacer.Size());
 ASSERT_EQ(true, lru_replacer.Evict(&value));
 ASSERT_EQ(5, value);
 ASSERT_EQ(1, lru_replacer.Size());

 // head: | head: <4,2,t>, <1,2,f>
 // Update access history for 1. Now we have [4,1]. Next victim is 4.
 lru_replacer.RecordAccess(1);
 lru_replacer.RecordAccess(1);
 lru_replacer.SetEvictable(1, true);

 // head: | head : <1,4,t> <4,2,t>
 ASSERT_EQ(2, lru_replacer.Size());
 ASSERT_EQ(true, lru_replacer.Evict(&value));
 ASSERT_EQ(value, 4);

 // head: | head : <1,4,t>
 ASSERT_EQ(1, lru_replacer.Size());
 lru_replacer.Evict(&value);
 ASSERT_EQ(value, 1);
 ASSERT_EQ(0, lru_replacer.Size());

 // These operations should not modify size
 ASSERT_EQ(false, lru_replacer.Evict(&value));
 ASSERT_EQ(0, lru_replacer.Size());
 lru_replacer.Remove(1);
 ASSERT_EQ(0, lru_replacer.Size());
}
}  // namespace bustub
