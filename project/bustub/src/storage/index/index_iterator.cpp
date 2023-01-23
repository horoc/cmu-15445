/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return cur_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return cur_page_->KeyValuePairAt(cur_index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (cur_page_ != nullptr) {
    if (cur_page_->GetSize() == cur_index_ + 1) {
      B_PLUS_TREE_LEAF_PAGE_TYPE *new_page = nullptr;
      if (cur_page_->GetNextPageId() != INVALID_PAGE_ID) {
        new_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bmp_->FetchPage(cur_page_->GetNextPageId()));
      }
      bmp_->UnpinPage(cur_page_->GetPageId(), false);
      cur_page_ = new_page;
      cur_index_ = 0;
    } else {
      cur_index_++;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return itr.cur_page_ == cur_page_ && itr.cur_index_ == cur_index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
