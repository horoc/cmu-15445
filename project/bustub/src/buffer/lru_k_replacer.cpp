//= == -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- == =  //
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

// LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(0), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  Node::Ptr ret;
  ret = history_.removeLastEvictableNode();
  if (ret != nullptr) {
    replacer_size_--;
    *frame_id = ret->frame_id_;
    m_.erase(ret->frame_id_);
    return true;
  }

  ret = cache_.removeLastEvictableNode();
  if (ret != nullptr) {
    replacer_size_--;
    *frame_id = ret->frame_id_;
    m_.erase(ret->frame_id_);
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  Node::Ptr frame = nullptr;

  if (m_.count(frame_id) == 0) {
    frame = Node::Ptr{new Node(frame_id)};
    m_[frame_id] = frame;
    history_.push_front(frame);
    curr_size_++;
  } else {
    frame = m_[frame_id];
  }

  frame->frequence_++;
  if (frame->frequence_ >= k_) {
    history_.remove(frame);
    cache_.push_front(frame);
    frame->in_cache_ = true;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (m_.count(frame_id) == 0) {
    throw bustub::Exception("invalid frame_id");
  }
  Node::Ptr frame = m_[frame_id];
  if (frame->evictable_ == set_evictable) {
    return;
  }

  if (set_evictable) {
    frame->evictable_ = true;
    replacer_size_++;
  } else {
    frame->evictable_ = false;
    replacer_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (m_.count(frame_id) == 0) {
    throw bustub::Exception("invalid frame_id");
  }
  Node::Ptr frame = m_[frame_id];
  if (frame->in_cache_) {
    cache_.remove(frame);
  } else {
    history_.remove(frame);
  }
  curr_size_--;
  if (frame->evictable_) {
    replacer_size_--;
  }
  m_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

}  // namespace bustub
