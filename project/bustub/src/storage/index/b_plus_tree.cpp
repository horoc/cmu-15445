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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
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
  auto leaf_page_id = GetLeafPageId(key);
  BPlusTreePage *page = GetPage(leaf_page_id);

  if (page != nullptr) {
    auto leaf_page = static_cast<LeafPage *>(page);
    ValueType val;
    if (leaf_page->LookupKey(key, val, comparator_)) {
      result->push_back(val);
      buffer_pool_manager_->UnpinPage(leaf_page_id, false);
      return true;
    }
  }

  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return false;
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
  auto leaf_page_id = GetLeafPageId(key);
  LeafPage *page = nullptr;
  // if empty tree
  if (leaf_page_id == INVALID_PAGE_ID) {
    page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&leaf_page_id));
    page->Init(leaf_page_id, INVALID_PAGE_ID, leaf_max_size_);
    root_page_id_ = leaf_page_id;
    UpdateRootPageId(1);
  } else {
    page = reinterpret_cast<LeafPage *>(GetPage(leaf_page_id));
  }

  // exist, directly return false
  ValueType tmp_val;
  if (page->LookupKey(key, tmp_val, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return false;
  }

  // insert to leaf
  if (!page->Insert(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
BPlusTreePage *BPLUSTREE_TYPE::GetPage(page_id_t page_id) {
  Page *pg = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage *>(pg);
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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
