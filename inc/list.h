#pragma once

#include <atomic>
#include <cstddef>
#include <arena.h>

struct ListNode {
  ListNode* next_;
  char data[0];
};


// 
class List {
public:
  inline List() : head_(nullptr) {}

  inline ListNode* get_iter() const {
    // TODO: 这样写正确吗
    return this->head_.load(std::memory_order_consume);
  }

  inline void insert(ListNode* node) {
    auto head = head_.load(std::memory_order_acquire);
    do {
      // TODO: 这样写正确吗 acq_rel will promise this store
      node->next_ = head;
    } while (!this->head_.compare_exchange_weak(
      head, node, 
      std::memory_order_acq_rel, std::memory_order_acquire
    ));
  }

  inline static ListNode* make_node(Arena& arena, size_t data_size) {
    return reinterpret_cast<ListNode*>(arena.Allocate(sizeof(ListNode*)+data_size));
  }

private:
  std::atomic<ListNode*> head_;
};