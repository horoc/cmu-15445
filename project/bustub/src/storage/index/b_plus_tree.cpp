#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  std::shared_lock<std::shared_mutex> lc(root_page_latch_);
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  auto leaf_page_id = GetLeafPageIdByCrabbingLock(key, transaction, false);
  BPlusTreePage *page = GetPage(leaf_page_id);

  bool find_key = false;
  if (page != nullptr) {
    auto leaf_page = static_cast<LeafPage *>(page);
    ValueType val;
    if (leaf_page->LookupKey(key, val, comparator_)) {
      result->push_back(val);
      find_key = true;
    }
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    ReleaseAndUnpinPageInTransaction(transaction, false);
  }
  return find_key;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // if empty tree
  if (IsEmpty()) {
    InitNewRootPage();
  }

  auto leaf_page_id = GetLeafPageIdByCrabbingLock(key, transaction, true, BPLUSTREE_TYPE::CheckSafeByPageType);
  LeafPage *page = GetLeafPage(leaf_page_id);

  // exist, directly return false
  ValueType tmp_val;
  if (page->LookupKey(key, tmp_val, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    ReleaseAndUnpinPageInTransaction(transaction, true);
    return false;
  }

  // insert to leaf
  if (!page->Insert(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    ReleaseAndUnpinPageInTransaction(transaction, true);
    return false;
  };

  // split page if necessary
  if (page->GetSize() >= page->GetMaxSize()) {
    page_id_t new_page_id;
    page_id_t old_page_id = leaf_page_id;
    LeafPage *new_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_page_id));
    new_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    LeafPage *old_page = page;
    int begin = page->GetSize() / 2;
    int end = page->GetSize();
    for (int i = begin; i < end; i++) {
      auto pair = page->KeyValuePairAt(i);
      new_page->Append(pair.first, pair.second);
    }
    old_page->IncreaseSize(-new_page->GetSize());

    // link leaf
    new_page->SetNextPageId(old_page->GetNextPageId());
    old_page->SetNextPageId(new_page_id);

    // insert new key to parent
    auto parent_id = old_page->GetParentPageId();
    InsertIntoInternalPage(parent_id, new_page->KeyAt(0), old_page_id, new_page_id);

    buffer_pool_manager_->UnpinPage(new_page_id, true);
    buffer_pool_manager_->UnpinPage(old_page_id, true);
  }

  ReleaseAndUnpinPageInTransaction(transaction, true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoInternalPage(page_id_t parent_page_id, const KeyType &child_key, page_id_t left_page_id,
                                            page_id_t right_page_id) {
  // child is root, and wants to insert new key to its parent
  if (parent_page_id == INVALID_PAGE_ID) {
    // new root page
    page_id_t new_root_page_id;
    InternalPage *new_root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_root_page_id));
    new_root_page->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_page->Append(child_key, right_page_id);
    new_root_page->SetValueAt(0, left_page_id);

    // set parent
    auto left_page = GetPage(left_page_id);
    auto right_page = GetPage(right_page_id);
    left_page->SetParentPageId(new_root_page_id);
    right_page->SetParentPageId(new_root_page_id);

    // update global root page
    UpdateRootPageId(root_page_id_ == INVALID_PAGE_ID ? 1 : 0);
    root_page_id_ = new_root_page_id;

    // unpin
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    buffer_pool_manager_->UnpinPage(left_page_id, true);
    buffer_pool_manager_->UnpinPage(right_page_id, true);
    return true;
  }

  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id));

  // split pre-check before inserting, since internal page can only store MaxSize - 1 items
  if (parent_page->GetSize() >= parent_page->GetMaxSize()) {
    page_id_t new_page_id;
    page_id_t old_page_id = parent_page_id;
    InternalPage *new_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_page_id));
    new_page->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    InternalPage *old_page = parent_page;
    // for example
    // old_page -> [<Null, A>, <K1, B>, <K2, C>, <K3, D>], size = 3
    // after split, old_page -> [<Null, A>, <K1,B>], new_page -> [<NULL,C>, <K3,D>], middle key is K2
    int size_after_insertion = old_page->GetSize() + 1;
    int begin = (size_after_insertion + 1) / 2;
    int end = old_page->GetSize() + 1;
    auto mid_kv = old_page->KeyValuePairAt(begin);
    for (int i = begin + 1; i < end; i++) {
      // move child page to new parent
      auto pair = old_page->KeyValuePairAt(i);
      auto child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pair.second));
      child->SetParentPageId(new_page_id);
      buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      new_page->Append(pair.first, pair.second);
    }
    old_page->IncreaseSize(-(new_page->GetSize() + 1));  // need to remove middle key
    new_page->SetValueAt(0, mid_kv.second);

    // append new key
    auto right_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_page_id));
    right_page->SetParentPageId(new_page_id);
    new_page->Append(child_key, right_page_id);

    page_id_t old_page_parent_id = old_page->GetParentPageId();
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    buffer_pool_manager_->UnpinPage(old_page_id, true);
    buffer_pool_manager_->UnpinPage(right_page_id, true);

    // recursive insert new_page to old_page's parent
    return InsertIntoInternalPage(old_page_parent_id, mid_kv.first, old_page_id, new_page_id);
  } else {
    auto right_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_page_id));
    right_page->SetParentPageId(parent_page_id);
    parent_page->Append(child_key, right_page_id);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    buffer_pool_manager_->UnpinPage(right_page_id, true);
    return true;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  page_id_t leaf_page_id = GetLeafPageId(key);
  if (leaf_page_id == INVALID_PAGE_ID) {
    return;
  }
  auto leaf_page = GetLeafPage(leaf_page_id);
  // delete
  if (!leaf_page->Delete(key, comparator_)) {
    // key not exist
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return;
  }
  buffer_pool_manager_->UnpinPage(leaf_page_id, true);

  // re-balancing from node if necessary
  ReBalancingPage(leaf_page_id, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReBalancingPage(page_id_t page_id, Transaction *transaction) {
  auto page = GetPage(page_id);
  bool is_leaf = page->IsLeafPage();

  // return if page has enough element
  if (page->GetSize() >= page->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return;
  }

  // deal with root page
  if (page->IsRootPage()) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    // check if root need to update after delete element
    ResetRootIfNecessary();
    return;
  }

  // try to borrow element from previous page
  page_id_t previous_page_id;
  int previous_parent_index;
  GetPreviousPageInfo(page_id, previous_page_id, previous_parent_index);
  if (previous_page_id != INVALID_PAGE_ID) {
    auto previous_page = GetPage(previous_page_id);
    if (previous_page->GetSize() + page->GetSize() >= 2 * page->GetMinSize()) {
      buffer_pool_manager_->UnpinPage(page_id, false);
      buffer_pool_manager_->UnpinPage(previous_page_id, false);
      BorrowOneElement(previous_page_id, page_id, is_leaf, false, transaction);
      return;
    } else {
      buffer_pool_manager_->UnpinPage(previous_page_id, false);
    }
  }

  // try to borrow element from next page
  page_id_t next_page_id;
  int next_parent_index;
  GetNextPageInfo(page_id, next_page_id, next_parent_index);
  if (next_page_id != INVALID_PAGE_ID) {
    auto next_page = GetPage(next_page_id);
    if (next_page->GetSize() + page->GetSize() >= 2 * page->GetMinSize()) {
      buffer_pool_manager_->UnpinPage(page_id, false);
      buffer_pool_manager_->UnpinPage(next_page_id, false);
      BorrowOneElement(page_id, is_leaf, true, next_parent_index, transaction);
      return;
    } else {
      buffer_pool_manager_->UnpinPage(next_page_id, false);
    }
  }

  auto parent_page_id = page->GetParentPageId();

  // try to merge with previous page
  if (previous_page_id != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    MergeElement(previous_page_id, page_id, previous_parent_index, transaction);
    auto parent_page = GetInternalPage(parent_page_id);
    parent_page->DeleteAt(previous_parent_index);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    if (parent_page->GetSize() < parent_page->GetMinSize()) {
      // recursive re-balancing parent
      ReBalancingPage(parent_page_id, transaction);
    }
    buffer_pool_manager_->UnpinPage(page_id, true);
    return;
  }

  // try to merge with next page
  if (next_page_id != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    MergeElement(next_page_id, page_id, next_parent_index, transaction);
    auto parent_page = GetInternalPage(parent_page_id);
    parent_page->DeleteAt(next_parent_index);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    if (parent_page->GetSize() < parent_page->GetMinSize()) {
      // recursive re-balancing parent
      ReBalancingPage(parent_page_id, transaction);
    }
    buffer_pool_manager_->UnpinPage(page_id, true);
    return;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeElement(page_id_t left_child, page_id_t right_child, int parent_key_index,
                                  Transaction *transaction) {
  auto left_child_page = GetLeafPage(left_child);
  auto right_child_page = GetLeafPage(right_child);
  auto parent_page_id = left_child_page->GetParentPageId();
  auto parent_page = GetInternalPage(parent_page_id);

  int begin = 0;
  int end = right_child_page->GetSize();
  if (!left_child_page->IsLeafPage()) {
    begin++;
    end++;
  }

  while (begin < end) {
    auto pair = right_child_page->KeyValuePairAt(begin);
    left_child_page->Append(pair.first, pair.second);
    begin++;
  }

  parent_page->DeleteAt(parent_key_index);
  bool has_enough_element = parent_page->GetSize() < parent_page->GetMinSize();
  buffer_pool_manager_->UnpinPage(left_child, true);
  buffer_pool_manager_->UnpinPage(right_child, true);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

  if (!has_enough_element) {
    ReBalancingPage(parent_page_id, transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowOneElement(page_id_t left_child, page_id_t right_child, int parent_key_index, bool is_leaf,
                                      Transaction *transaction) {
  if (is_leaf) {
    auto left_child_page = GetLeafPage(left_child);
    auto right_child_page = GetLeafPage(right_child);
    auto parent_page = GetInternalPage(left_child_page->GetParentPageId());
    if (left_child_page->GetSize() > right_child_page->GetSize()) {
      // move last element of left child to right child's first position
      auto pair = left_child_page->KeyValuePairAt(left_child_page->GetSize() - 1);
      left_child_page->DeleteAt(left_child_page->GetSize() - 1);
      right_child_page->InsertAt(0, pair.first, pair.second);
    } else {
      // move first element of right child to left child's last position
      auto pair = right_child_page->KeyValuePairAt(0);
      right_child_page->DeleteAt(0);
      left_child_page->InsertAt(left_child_page->GetSize(), pair.first, pair.second);
    }

    // update parent
    parent_page->SetKeyAt(parent_key_index, right_child_page->KeyAt(0));

    buffer_pool_manager_->UnpinPage(left_child, true);
    buffer_pool_manager_->UnpinPage(right_child, true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  } else {
    auto left_child_page = GetInternalPage(left_child);
    auto right_child_page = GetInternalPage(right_child);
    auto parent_page = GetInternalPage(left_child_page->GetParentPageId());
    auto parent_key = parent_page->KeyAt(parent_key_index);
    if (left_child_page->GetSize() > right_child_page->GetSize()) {
      // move last element of left child to right child's first position
      right_child_page->SetKeyAt(0, parent_key);
      auto pair = left_child_page->KeyValuePairAt(left_child_page->GetSize() - 1);
      left_child_page->DeleteAt(left_child_page->GetSize() - 1);
      right_child_page->InsertAt(0, pair.first, pair.second);
      parent_page->SetKeyAt(parent_key_index, pair.first);
    } else {
      // move first element of right child to left child's last position
      auto pair = right_child_page->KeyValuePairAt(0);
      left_child_page->InsertAt(left_child_page->GetSize(), parent_key, pair.second);
      parent_page->SetKeyAt(parent_key_index, right_child_page->KeyAt(1));
      right_child_page->DeleteAt(0);
    }

    // update parent
    parent_page->SetKeyAt(parent_key_index, right_child_page->KeyAt(0));

    buffer_pool_manager_->UnpinPage(left_child, true);
    buffer_pool_manager_->UnpinPage(right_child, true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::ResetRootIfNecessary() {
  /*
   * Cases trigger root updating
   * 1. root is leaf and doesn't contain any element
   * 2. root is internal and has only one child
   *
   * It's impossible that root is internal and doesn't contain any child, since case 2 will update root to leaf page
   */

  if (root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto root_page = GetPage(root_page_id_);
  // 1. root is leaf and doesn't contain any element
  if (root_page->IsLeafPage() && root_page->GetSize() == 0) {
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }

  // 2. root is internal and has only one child
  if (!root_page->IsLeafPage()) {
    auto page = reinterpret_cast<InternalPage *>(root_page);
    // has only one child : contains one key and right pointer is INVALID_PAGE_ID
    if (page->GetSize() == 1 && page->ValueAt(1) == INVALID_PAGE_ID) {
      page_id_t child_id = page->ValueAt(0);
      buffer_pool_manager_->UnpinPage(root_page_id_, false);
      root_page_id_ = child_id;
      UpdateRootPageId(0);

      auto child_page = GetLeafPage(child_id);
      child_page->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(child_id, true);
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetPreviousPageInfo(page_id_t page_id, page_id_t &previous_page_id, int &parent_key_index) {
  previous_page_id = INVALID_PAGE_ID;

  auto page = GetPage(page_id);
  page_id_t parent_page_id = page->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return;
  }

  auto parent_page = GetInternalPage(page_id);
  int pre_index = -1;
  int cur_index = 0;
  while (cur_index <= parent_page->GetSize()) {
    if (parent_page->ValueAt(cur_index) == page_id) {
      parent_key_index = cur_index;
      previous_page_id = parent_page->ValueAt(pre_index);
      break;
    }
    pre_index = cur_index;
    cur_index++;
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, false);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetNextPageInfo(page_id_t page_id, page_id_t &next_page_id, int &parent_key_index) {
  next_page_id = INVALID_PAGE_ID;

  auto page = GetPage(page_id);

  page_id_t parent_page_id = page->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return;
  }

  auto parent_page = GetInternalPage(page_id);
  int cur_index = 0;
  int next_index = 1;
  while (next_index <= parent_page->GetSize()) {
    if (parent_page->ValueAt(cur_index) == page_id) {
      parent_key_index = next_index;
      next_page_id = parent_page->ValueAt(next_index);
      break;
    }
    cur_index = next_index;
    next_index++;
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, false);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto first_leaf_page_id = GetFirstLeafPageId();
  auto page = GetLeafPage(first_leaf_page_id);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto target_leaf_page_id = GetLeafPageId(key);
  auto page = GetLeafPage(target_leaf_page_id);
  auto index = page->PositionOfNearestKey(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
auto BPLUSTREE_TYPE::GetPageAndLock(page_id_t page_id, Transaction *transaction, bool exclusive) -> PageType * {
  Page *pg = buffer_pool_manager_->FetchPage(page_id);
  if (pg == nullptr) {
    return nullptr;
  }

  if (exclusive) {
    pg->WLatch();
  } else {
    pg->RLatch();
  }

  if (transaction != nullptr) {
    transaction->AddIntoPageSet(pg);
  }

  return reinterpret_cast<PageType *>(pg);
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::GetPage(page_id_t page_id) {
  Page *pg = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage *>(pg);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(page_id_t page_id) -> LeafPage * {
  Page *pg = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<LeafPage *>(pg);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetInternalPage(page_id_t page_id) -> InternalPage * {
  Page *pg = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<InternalPage *>(pg);
}

INDEX_TEMPLATE_ARGUMENTS
page_id_t BPLUSTREE_TYPE::GetLeafPageId(const KeyType &key) {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  BPlusTreePage *page = GetPage(root_page_id_);
  page_id_t cur_page_id = root_page_id_;
  while (page != nullptr && !page->IsLeafPage()) {
    auto internalPage = static_cast<InternalPage *>(page);
    int pos = internalPage->GetKeySlotPosition(key, comparator_);
    page_id_t next_page_id = static_cast<page_id_t>(internalPage->ValueAt(pos));
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    cur_page_id = next_page_id;
    page = GetPage(cur_page_id);
  }

  buffer_pool_manager_->UnpinPage(cur_page_id, false);
  return cur_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
page_id_t BPLUSTREE_TYPE::GetLeafPageIdByCrabbingLock(const KeyType &key, Transaction *transaction, bool exclusive,
                                                      SafeCheckFunction is_safe) {
  if (transaction == nullptr) {
    return GetLeafPageId(key);
  }

  if (IsEmpty()) {
    return INVALID_PAGE_ID;
  }

  auto page = GetPage(root_page_id_);
  LockPage(page, exclusive);
  AddPageToTransaction(page, transaction);
  page_id_t cur_page_id = root_page_id_;
  while (page != nullptr && !page->IsLeafPage()) {
    auto internalPage = static_cast<InternalPage *>(page);
    int pos = internalPage->GetKeySlotPosition(key, comparator_);

    page_id_t next_page_id = static_cast<page_id_t>(internalPage->ValueAt(pos));
    if (next_page_id == INVALID_PAGE_ID) {
      break;
    }
    auto next_page = GetPage(next_page_id);
    LockPage(next_page, exclusive);
    if (is_safe(next_page, false)) {
      ReleaseAndUnpinPageInTransaction(transaction, exclusive);
    }
    AddPageToTransaction(next_page, transaction);

    page = next_page;
    cur_page_id = next_page_id;
  }

  return cur_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
page_id_t BPLUSTREE_TYPE::GetFirstLeafPageId() {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  BPlusTreePage *page = GetPage(root_page_id_);
  page_id_t cur_page_id = root_page_id_;
  while (page != nullptr && !page->IsLeafPage()) {
    auto internalPage = static_cast<InternalPage *>(page);
    page_id_t next_page_id = static_cast<page_id_t>(internalPage->ValueAt(0));
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    cur_page_id = next_page_id;
    page = GetPage(cur_page_id);
  }

  buffer_pool_manager_->UnpinPage(cur_page_id, false);
  return cur_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InitNewRootPage() {
  root_page_latch_.lock();
  // double check
  if (root_page_id_ == INVALID_PAGE_ID) {
    page_id_t page_id;
    LeafPage *page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&page_id));
    page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
    root_page_id_ = page_id;
    UpdateRootPageId(1);
  }
  root_page_latch_.unlock();
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
