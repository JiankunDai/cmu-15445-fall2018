/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {
  head = new LRUNode<T>;
  tail = new LRUNode<T>;
  head->prev = nullptr;
  head->next = tail;
  tail->prev = head;
  tail->next = nullptr;
  size = 0;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {
  delete head;
  delete tail;
}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> lock(mtx);
  LRUNode<T> *lruNode;
  auto iter = cache.find(value);
  if (iter != cache.end()) {
    lruNode = iter->second;
    // detach from end
    detach(lruNode);
    // attch to begin
    attach(lruNode);
  } else {
    lruNode = new LRUNode<T>;
    lruNode->data = value;
    attach(lruNode);
    size++;
    cache.insert(std::make_pair(value, lruNode));
  }
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> lock(mtx);
  if (Size() == 0)
    return false;
  // pop the tail member
  LRUNode<T> *lruNode = tail->prev;
  value = lruNode->data;
  detach(lruNode);
  cache.erase(value);
  delete lruNode;
  size--;
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> lock(mtx);
  LRUNode<T> *lruNode;
  auto iter = cache.find(value);
  if (iter != cache.end()) {
    lruNode = iter->second;
    lruNode->prev->next = lruNode->next;
    lruNode->next->prev = lruNode->prev;
    delete lruNode;
    size--;
    cache.erase(value);
    return true;
  }
  return false;
}

template <typename T> size_t LRUReplacer<T>::Size() { return size; }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
