/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include "buffer/replacer.h"
#include <unordered_map>
#include <mutex>

namespace cmudb {

template <typename T> struct LRUNode {
  T data;
  LRUNode *prev, *next;
};

template <typename T> class LRUReplacer : public Replacer<T> {
public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

private:
  std::mutex mtx;
  std::unordered_map<T, LRUNode<T> *> cache;
  LRUNode<T> *head, *tail;
  size_t size;
  void attach(LRUNode<T> *node) {
    head->next->prev = node;
    node->next = head->next;
    node->prev = head;
    head->next = node;
  }
  void detach(LRUNode<T> *node) {
    node->next->prev = node->prev;
    node->prev->next = node->next;
    node->next = nullptr;
    node->prev = nullptr;
  }
};

} // namespace cmudb
