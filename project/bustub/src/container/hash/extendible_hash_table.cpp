//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::shared_ptr<Bucket>(new Bucket(bucket_size)));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  std::shared_ptr<Bucket> bucket_ptr = dir_[IndexOf(key)];
  if (bucket_ptr == nullptr) {
    return false;
  }
  return bucket_ptr->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock{latch_};
  auto key_hash = IndexOf(key);
  std::shared_ptr<Bucket> bucket_ptr = dir_[key_hash];
  if (bucket_ptr != nullptr) {
    return bucket_ptr->Remove(key);
  }
  return false;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock{latch_};
  auto key_hash = IndexOf(key);
  std::shared_ptr<Bucket> bucket_ptr = dir_[key_hash];

  while (bucket_ptr->IsFull()) {
    // update value
    V val;
    if (bucket_ptr->Find(key, val)) {
      bucket_ptr->Insert(key, value);
      return;
    }

    // split

    // no more position for new bucket, need grow container
    if (bucket_ptr->GetDepth() == GetGlobalDepthInternal()) {
      for (auto i = 0; i < num_buckets_; i++) {
        dir_.push_back(dir_[i]);
      }
      num_buckets_ *= 2;
      global_depth_++;
    }

    std::shared_ptr<Bucket> p0(new Bucket(bucket_size_, bucket_ptr->GetDepth() + 1));
    std::shared_ptr<Bucket> p1(new Bucket(bucket_size_, bucket_ptr->GetDepth() + 1));
    int high_bit = (1 << bucket_ptr->GetDepth());

    auto items = bucket_ptr->GetItems();
    for (auto it = items.begin(); it != items.end(); ++it) {
      int hash = std::hash<K>()(it->first);
      /*
        for example, before split, local depth = 2, means key with same low 2 bit will put into this bucket
        like:
          1010010 10
          1010111 10
          ...
        after split, local depth will be 3,
          101001 010
          101011 110

        in spliting, we only need to check the 3rd to the last bit, if 0 split to p0, and 1 split to p1
      */
      if (hash & high_bit) {
        p1->Insert(it->first, it->second);
      } else {
        p0->Insert(it->first, it->second);
      }
    }
    // set p0 and p1 to right position
    for (int i = std::hash<K>()(key) & (high_bit - 1); i < static_cast<int>(dir_.size()); i += high_bit) {
      if (i & high_bit) {
        dir_[i] = p1;
      } else {
        dir_[i] = p0;
      }
    }
    bucket_ptr = dir_[IndexOf(key)];
  }

  bucket_ptr->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      size_--;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }
  list_.push_back(std::pair<K, V>{key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
