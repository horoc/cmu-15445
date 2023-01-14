//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

bool BufferPoolManagerInstance::GetPageFrameIdFromFreeListOrPool(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Evict(frame_id)) {
      return false;
    }
  }
  return true;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // get a new page from free list or evict from pool
  frame_id_t frame_id;
  if (!GetPageFrameIdFromFreeListOrPool(&frame_id)) {
    return nullptr;
  }

  // flush if dirty
  Page *pg = pages_ + frame_id;
  if (pages_->IsDirty()) {
    disk_manager_->WritePage(pages_->page_id_, pages_->GetData());
  }

  // reset data
  replacer_->Remove(frame_id);
  page_table_->Remove(pg->page_id_);
  pg->ResetMemory();
  pg->page_id_ = AllocatePage();
  page_table_->Insert(pg->page_id_, frame_id);

  // pin
  pg->pin_count_ = 1;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  *page_id = pg->page_id_;
  return pg;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    if (!GetPageFrameIdFromFreeListOrPool(&frame_id)) {
      return nullptr;
    }

    // flush if dirty
    Page *pg = pages_ + frame_id;
    if (pages_->IsDirty()) {
      disk_manager_->WritePage(pages_->page_id_, pages_->GetData());
    }

    // read data
    replacer_->Remove(frame_id);
    page_table_->Remove(pg->page_id_);
    disk_manager_->ReadPage(page_id, pg->GetData());
    pg->page_id_ = page_id;
    page_table_->Insert(pg->page_id_, frame_id);

    // pin
    pg->pin_count_ = 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    // update table
    page_table_->Insert(pg->page_id_, frame_id);
    return pg;
  } else {
    Page *pg = pages_ + frame_id;
    // has cached this page, record and return directly
    pages_->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return pg;
  }
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { return false; }

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { return false; }

void BufferPoolManagerInstance::FlushAllPgsImp() {}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { return false; }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
