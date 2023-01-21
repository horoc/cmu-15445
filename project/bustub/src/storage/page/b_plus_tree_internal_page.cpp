//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index < GetSize());
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  assert(index < GetSize());
  array_[index].second = value;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index < GetSize());
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueOfNearestKey(KeyType key, const KeyComparator &comparator) -> ValueType {
  // first key always empty, there should be n key and n + 1 next page pointer store in array_
  int begin = 1;
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
      return array_[mid].second;
    }
  }

  // all key greater than input key
  if (end == 1) {
    return array_[end].second;
  }
  // all key smaller than input key
  return array_[begin].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PositionOfNearestKey(KeyType key, const KeyComparator &comparator) -> int {
  // first key always empty, there should be n key and n + 1 next page pointer store in array_
  int begin = 1;
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
  if (end == 1) {
    return 1;
  }
  // all key smaller than input key
  return begin;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comparator)
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Append(const KeyType &key, const ValueType &val) {
  MappingType tmp(key, val);
  array_[GetSize()] = tmp;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
MappingType &B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyValuePairAt(int index) {
  assert(index < GetSize() + 1 && index > 0);
  return array_[index];
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
