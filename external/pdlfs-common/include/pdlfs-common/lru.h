#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdlib.h>

#include "pdlfs-common/hash.h"
#include "pdlfs-common/map.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {

template <typename T = void>
struct LRUEntry {
  T* value;
  void (*deleter)(const Slice&, T* value);
  LRUEntry<T>* next_hash;
  LRUEntry<T>* next;
  LRUEntry<T>* prev;
  size_t charge;
  size_t key_length;
  uint32_t refs;
  uint32_t hash;  // Hash of key(); used for fast partitioning and comparisons
  char key_data[1];  // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// A simple LRU cache implementation that requires external synchronization.
template <typename T = void>
class LRUCache {
  typedef LRUEntry<T> E;

 private:
  // Max cache size.
  size_t capacity_;
  // Current capacity consumption.
  size_t usage_;

  // Dummy head of LRU list.
  // lru_.prev is newest entry, lru_.next is oldest entry.
  E lru_;

  HashTable<E> table_;

  // No copying allowed
  LRUCache(const LRUCache&);
  LRUCache& operator=(const LRUCache&);

  void Unref(E* e) {
    assert(e->refs > 0);
    e->refs--;
    if (e->refs <= 0) {
      usage_ -= e->charge;
      (*e->deleter)(e->key(), e->value);
      free(e);
    }
  }

  void LRU_Remove(E* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
  }

  void LRU_Append(E* e) {
    // Make "e" newest entry by inserting just before lru_
    e->next = &lru_;
    e->prev = lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
  }

 public:
  LRUCache(size_t capacity = 0) : capacity_(capacity), usage_(0) {
    // Make empty circular linked list
    lru_.next = &lru_;
    lru_.prev = &lru_;
  }

  ~LRUCache() {
    for (E* e = lru_.next; e != &lru_;) {
      E* next = e->next;
      assert(e->refs == 1);  // Error if caller has an unreleased handle
      Unref(e);
      e = next;
    }
  }

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t c) { capacity_ = c; }

  E* Insert(const Slice& key, uint32_t hash, T* value, size_t charge,
            void (*deleter)(const Slice& key, T* value)) {
    E* e = static_cast<E*>(malloc(sizeof(E) - 1 + key.size()));
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->hash = hash;
    e->refs = 2;  // One from LRUCache, one for the returned handle
    memcpy(e->key_data, key.data(), key.size());
    LRU_Append(e);
    usage_ += charge;

    E* old = table_.Insert(e);
    if (old != NULL) {
      LRU_Remove(old);
      Unref(old);
    }

    while (usage_ > capacity_ && lru_.next != &lru_) {
      E* old = lru_.next;
      LRU_Remove(old);
      table_.Remove(old->key(), old->hash);
      Unref(old);
    }

    return e;
  }

  E* Lookup(const Slice& key, uint32_t hash) {
    E* e = table_.Lookup(key, hash);
    if (e != NULL) {
      e->refs++;
      LRU_Remove(e);
      LRU_Append(e);
    }
    return e;
  }

  E* InsertIfAbsent(const Slice& key, uint32_t hash, T* value, size_t charge,
                    void (*deleter)(const Slice& key, T* value)) {
    E* e = table_.Lookup(key, hash);
    if (e != NULL) {
      return NULL;
    } else {
      return Insert(key, hash, value, charge, deleter);
    }
  }

  void Prune() {
    for (E* e = lru_.next; e != &lru_;) {
      E* next = e->next;
      if (e->refs == 1) {
        table_.Remove(e->key(), e->hash);
        LRU_Remove(e);
        Unref(e);
      }
      e = next;
    }
  }

  void Erase(const Slice& key, uint32_t hash) {
    E* e = table_.Remove(key, hash);
    if (e != NULL) {
      LRU_Remove(e);
      Unref(e);
    }
  }

  bool Empty() { return (lru_.next == &lru_) && (lru_.prev == &lru_); }

  void Release(E* entry) {
    Unref(entry);  // Do not use entry hereafter
  }
};

}  // namespace pdlfs
