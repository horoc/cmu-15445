//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(parent_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index < GetSize());
  return array_[index].first;
}

// get value of the which is equals to input key
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookupKey(KeyType key, ValueType &val, const KeyComparator &comparator) -> bool {
  int begin = 0;
  int end = GetSize();
  while (begin <= end) {
    int mid = begin + (end - begin) / 2;
    int cmp_ret = comparator(array_[mid].first, key);
    if (cmp_ret < 0) {
      begin = mid + 1;
    } else if (cmp_ret > 0) {
      end = mid - 1;
    } else {
      val = array_[mid].second;
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PositionOfNearestKey(KeyType key, const KeyComparator &comparator) -> int {
  int begin = 0;
  int end = GetSize();
  while (begin <= end) {
    int mid = begin + (end - begin) / 2;
    int cmp_ret = comparator(array_[mid].first, key);
    if (cmp_ret < 0) {
      begin = mid + 1;
    } else if (cmp_ret > 0) {
      end = mid - 1;
    } else {
      // directly return since all keys are unique
      return mid;
    }
  }

  // all key greater than input key
  if (end == 0) {
    return 0;
  }
  // all key smaller than input key
  return begin;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comparator)
    -> bool {
  int pos = PositionOfNearestKey(key, comparator);
  if (comparator(array_[pos].first, key) == 0) {
    return false;
  }
  int index = pos;
  for (int i = GetSize(); i > pos; i--) {
    array_[i] = array_[i - 1];
  }
  MappingType tmp(key, val);
  array_[pos] = tmp;
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Append(const KeyType &key, const ValueType &val) {
  MappingType tmp(key, val);
  array_[GetSize()] = tmp;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::KeyValuePairAt(int index) {
  assert(index < GetSize() && index > 0);
  return array_[index];
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
