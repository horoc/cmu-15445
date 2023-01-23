//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator() : bmp_(nullptr), cur_page_(nullptr), cur_index_(0){};
  IndexIterator(BufferPoolManager *bpm, B_PLUS_TREE_LEAF_PAGE_TYPE *cur_page, int index = 0)
      : bmp_(bpm), cur_page_(cur_page), cur_index_(index){};
  ~IndexIterator() {
    if (cur_page_ != nullptr) {
      bmp_->UnpinPage(cur_page_->GetPageId(), false);
    }
  }

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  BufferPoolManager *bmp_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *cur_page_;
  int cur_index_;
  // add your own private member variables here
};

}  // namespace bustub
