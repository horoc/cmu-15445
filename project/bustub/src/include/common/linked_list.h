#include <memory>

namespace bustub {
template <typename T>
struct ListNode {
  typedef std::shared_ptr<ListNode<T>> Ptr;
  ListNode(const T &val) : val_(val){};
  T val_;
  Ptr prev_;
  Ptr next_;
};

template <typename T>
class LinkedList {
 public:
  LinkedList() = default;
  ~LinkedList() = default;
  void offer(ListNode<T>::Ptr ptr) {
    if (head_ == nullptr) {
      head_ = ptr;
      return;
    }

    ptr->next_ = head_;
    ptr->prev = nullptr;

    head_->prev = ptr;
    if (tail_ == nullptr) {
      tail = head_;
    }
    head_ = ptr;
  }

  ListNode<T>::Ptr poll() {
    if (size_ == 0) {
      return nullptr;
    }
    // only one node
    if (tail_ == nullptr) {
      size--;
      ListNode<T>::Ptr tmp = head_;
      head_ = nullptr;
      return tmp;
    }

    // poll tail
    ListNode<T>::Ptr tmp = tail_;
    tail_ = tail_.prev;
    return tail_;
  }

  // we assume LinkedList contains node without check
  void removeNode(ListNode<T>::Ptr node) {
    if (node == head_) {
      head_ = head_->next_;
      return;
    }

    if (node == tail_) {
      tail_ = tail_->prev;
      return;
    }
  }

 private:
  ListNode<T>::Ptr head_;
  ListNode<T>::Ptr tail_;
  int size_;
};
}  // namespace bustub