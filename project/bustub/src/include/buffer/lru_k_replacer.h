//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

struct Node {
  typedef std::shared_ptr<Node> Ptr;
  Node(frame_id_t frame_id) : frame_id_(frame_id){};
  frame_id_t frame_id_;
  size_t frequence_{0};
  bool in_cache_;
  bool evictable_;
  Ptr prev_;
  Ptr next_;
};

class List {
 public:
  List() : head_(new Node(0)) {}
  Node::Ptr getHead() { return head_->next_; }

  int size() { return size_; }

  void remove(Node::Ptr node) {
    if (node == tail_) {
      tail_ = node->prev_;
    }

    node->prev_->next_ = node->next_;
    if (node->next_ != nullptr) {
      node->next_->prev_ = node->prev_;
    }
    node->prev_ = nullptr;
    node->next_ = nullptr;
    size_--;
  }

  Node::Ptr removeLastEvictableNode() {
    if (size_ == 0) {
      return nullptr;
    }

    Node::Ptr cur = tail_;
    while (cur != head_) {
      if (cur->evictable_) {
        remove(cur);
        size_--;
        return cur;
      }
      cur = cur->prev_;
    }
    return nullptr;
  }

  void push_front(Node::Ptr node) {
    node->next_ = head_->next_;
    if (node->next_ != nullptr) {
      node->next_->prev_ = node;
    }

    head_->next_ = node;
    node->prev_ = head_;

    if (node->next_ == nullptr) {
      tail_ = node;
    }
    size_++;
  }

 private:
  int size_;
  Node::Ptr head_;
  Node::Ptr tail_;
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::unordered_map<frame_id_t, Node::Ptr> m_;
  std::mutex latch_;
  List history_;
  List cache_;
};

}  // namespace bustub
