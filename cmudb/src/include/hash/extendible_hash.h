/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>

#include "hash/hash_table.h"

namespace cmudb {

template <typename K, typename V>
class HashPage {
 public:
  int d;
  size_t pageSize;
  std::unordered_map<K, V> m;
  HashPage(size_t size) : pageSize(size) { d = 0; };
  bool IsFull() { return m.size() >= pageSize; };
  void Put(const K& k, const V& v) { m[k] = v; };
  bool Get(const K& k, V& value) {
    auto it = m.find(k);
    if (it != m.end()) {
      value = (*it).second;
      return true;
    }
    return false;
  };
  bool Remove(const K& key) { return m.erase(key) > 0; }
  bool operator==(const HashPage& b) const { return d == b.d && m == b.m; }
};

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
 public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K& key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K& key, V& value) override;
  bool Remove(const K& key) override;
  void Insert(const K& key, const V& value) override;

 private:
  int gd;
  std::vector<HashPage<K, V>*> pp;
  std::mutex mtx;
  void grow() {
    gd += 1;
    size_t s = pp.size();
    for (size_t i = 0; i < s; i++) {
      pp.push_back(pp[i]);
    }
  }
};
}  // namespace cmudb
