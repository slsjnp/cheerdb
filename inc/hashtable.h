#pragma once

#include <optional>
#include <vector>

#include "list.h"
#include "arena.h"

template<typename K, typename V, size_t B>
class HashTable {
public:
  constexpr size_t N = 1 << B;

  HashTable()
  {

  }

  void set(const K& key, const V& value)
  {
    auto slot_id = get_slot_id(key);
    assume(0 <= slot_id && slot_id < N);

    auto& bucket = buckets_[slot_id];
    auto node = bucket.make_node(arena_, sizeof(K) + sizeof(V));
    bucket.insert(node);
  }

  std::optional<const V&> get(const K& key)
  {
    auto slot_id = get_slot_id(key);
    assume(0 <= slot_id && slot_id < N);

    auto& bucket = buckets_[slot_id];
    auto head = bucket->get_iter();
    while (head) {
      auto& k = *reinterpret_cast<const K*>(head->data);
      auto& v = *reinterpret_cast<const K*>(head->data + sizeof(K));
      if (k == key) return v;
      head = head->next_;
    }
    return {};
  }

  std::vector<V> get_all(const K& key)
  {
    auto slot_id = get_slot_id(key);
    assume(0 <= slot_id && slot_id < N);

    auto res = std::vector<const V&>();

    auto& bucket = buckets_[slot_id];
    auto head = bucket->get_iter();
    while (head) {
      auto& k = *reinterpret_cast<const K*>(head->data);
      auto& v = *reinterpret_cast<const K*>(head->data + sizeof(K));
      if (k == key) res.emplace_back(v);
      head = head->next_;
    }

    return res;
  }

private:
  static size_t get_slot_id(const K& key)
  {
    return static_cast<size_t>(K::hash(key) % N);
  }

  Arena arena_;
  List buckets_[1 << B];
};