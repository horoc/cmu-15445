//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using SafeCheckFunction = std::function<bool(BPlusTreePage *)>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  BPlusTreePage *GetPage(page_id_t page_id);

  template <typename PageType>
  auto GetPageAndLock(page_id_t page_id, Transaction *transaction, bool exclusive) -> PageType *;

  auto GetLeafPage(page_id_t page_id) -> LeafPage *;

  auto GetInternalPage(page_id_t page_id) -> InternalPage *;

  page_id_t GetLeafPageId(const KeyType &key);

  page_id_t GetLeafPageIdByCrabbingLock(const KeyType &key, Transaction *transaction, bool exclusive,
                                        SafeCheckFunction is_safe = AlwaysSafe);

  page_id_t GetFirstLeafPageId();

  void GetPreviousPageInfo(page_id_t page_id, page_id_t &previous_page_id, int &parent_key_index);

  void GetNextPageInfo(page_id_t page_id, page_id_t &next_page_id, int &parent_key_index);

  void ReBalancingPage(page_id_t page_id, Transaction *transaction);

  void BorrowOneElement(page_id_t left_child, page_id_t right_child, int parent_key_index, bool is_leaf,
                        Transaction *transaction);

  void MergeElement(page_id_t left_child, page_id_t right_child, int parent_key_index, Transaction *transaction);

  bool ResetRootIfNecessary();

  bool InsertIntoInternalPage(page_id_t parent_page_id, const KeyType &child_key, page_id_t left_page_id,
                              page_id_t right_page_id);

  void InitNewRootPage();

  /*
   * Transaction related function #############################################################################
   */

  inline void LockPage(BPlusTreePage *page, bool exclusive) {
    if (exclusive) {
      reinterpret_cast<Page *>(page)->WLatch();
    } else {
      reinterpret_cast<Page *>(page)->RLatch();
    }
  }

  inline void UnlockPage(BPlusTreePage *page, bool exclusive) {
    if (exclusive) {
      reinterpret_cast<Page *>(page)->WUnlatch();
    } else {
      reinterpret_cast<Page *>(page)->RUnlatch();
    }
  }

  inline void RecordPageInTransaction(BPlusTreePage *page, Transaction *transaction) {
    if (transaction != nullptr && page != nullptr) {
      transaction->AddIntoPageSet(reinterpret_cast<Page *>(page));
    }
  }

  inline void RecordDeletePageInTransaction(BPlusTreePage *page, Transaction *transaction) {
    if (transaction != nullptr && page != nullptr) {
      transaction->AddIntoDeletedPageSet(page->GetPageId());
    }
  }

  inline void ReleasePageInTransaction(Transaction *transaction, bool exclusive) {
    if (transaction == nullptr) {
      return;
    }
    auto pages = transaction->GetPageSet();
    for (auto it = pages->begin(); it != pages->end(); ++it) {
      Page *pg = *it;
      if (exclusive) {
        pg->WUnlatch();
      } else {
        pg->RUnlatch();
      }
    }
    pages->clear();
  }

  inline void ReleaseAndUnpinPageInTransaction(Transaction *transaction, bool exclusive) {
    if (transaction == nullptr) {
      return;
    }
    auto pages = transaction->GetPageSet();
    for (auto it = pages->begin(); it != pages->end(); ++it) {
      Page *pg = *it;
      if (exclusive) {
        pg->WUnlatch();
      } else {
        pg->RUnlatch();
      }
      // always false, dirty logic should be controlled by tree logic
      // and if a page is marked by dirty, it won't update back to non-dirty page until flush to disk
      buffer_pool_manager_->UnpinPage(pg->GetPageId(), false);
    }
    pages->clear();

    auto delete_pages = transaction->GetDeletedPageSet();
    for (auto it = delete_pages->begin(); it != delete_pages->end(); ++it) {
      auto page_id = *it;
      buffer_pool_manager_->DeletePage(page_id);
    }
    delete_pages->clear();
  }

  static inline bool AlwaysSafe(BPlusTreePage *) { return true; }

  static inline bool IsAddElementSafe(BPlusTreePage *page) {
    if (page->IsLeafPage()) {
      return page->GetSize() < page->GetMaxSize() - 1;
    } else {
      return page->GetSize() < page->GetMaxSize();
    }
  }

  static inline bool IsDeleteElementSafe(BPlusTreePage *page) { return page->GetSize() > page->GetMinSize(); }

  /*
   * Debug function #############################################################################
   */

  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

 private:
  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  mutable std::shared_mutex root_page_latch_;
  mutable std::mutex test_latch_;
};
}  // namespace bustub