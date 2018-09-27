#include <list>

#include <functional>
#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) {
  auto p = new HashPage<K, V>(size);
  pp.push_back(p);
  gd = 0;
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K& key) {
  std::hash<K> keyHash;
  return keyHash(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return gd;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  return pp.at(bucket_id)->d;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return pp.size();
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K& key, V& value) {
  size_t h = HashKey(key);
  int bucketId = h & ((1 << gd) - 1);
  auto p = pp.at(bucketId);
  return p->Get(key, value);
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K& key) {
  std::lock_guard<std::mutex> lock(mtx);
  size_t h = HashKey(key);
  int bucketId = h & ((1 << gd) - 1);
  auto p = pp.at(bucketId);
  return p->Remove(key);
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K& key, const V& value) {
  std::lock_guard<std::mutex> lock(mtx);
  size_t h = HashKey(key);
  int bucketId = h & ((1 << gd) - 1);
  auto p = pp.at(bucketId);
  if (p->IsFull() && p->d == gd) {
    grow();
  }
  if (p->IsFull() && p->d < gd) {
    p->Put(key, value);
    auto p1 = new HashPage<K, V>(p->pageSize);
    auto p2 = new HashPage<K, V>(p->pageSize);
    for (auto it : p->m) {
      auto k2 = it.first;
      auto v2 = it.second;
      size_t h2 = HashKey(k2);
      h2 = h2 & ((1 << gd) - 1);
      if (((h2 >> p->d) & 1) == 1) {
        p2->Put(k2, v2);
      } else {
        p1->Put(k2, v2);
      }
    }

    for (size_t i = 0; i < pp.size(); i++) {
      if (*(pp[i]) == *p) {
        if (((i >> p->d) & 1) == 1) {
          pp[i] = p2;
        } else {
          pp[i] = p1;
        }
      }
    }
    p1->d = p->d + 1;
    p2->d = p->d + 1;
  } else {
    p->Put(key, value);
  }
}

template class ExtendibleHash<page_id_t, Page*>;
template class ExtendibleHash<Page*, std::list<Page*>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
}  // namespace cmudb
