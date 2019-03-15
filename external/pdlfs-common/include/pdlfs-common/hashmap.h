/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/hash.h"
#include "pdlfs-common/slice.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

namespace pdlfs {

// Each entry is a variable length heap-allocated structure that points
// to a user allocated hashable data object. This entry is simultaneously used
// by a hash table and a logical linked link to form a linked hash map.
template <typename T = void>
struct HashEntry {
  T* value;
  HashEntry<T>* next_hash;
  HashEntry<T>* next;
  HashEntry<T>* prev;
  size_t key_length;
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

// A simple hash table implementation that removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., read random speeds up by ~5% over the g++
// 4.4.3's builtin hash table.
template <typename E>
class HashTable {
 public:
  HashTable() : length_(0), elems_(0), list_(NULL) { Resize(); }

  ~HashTable() { delete[] list_; }

  // Return a pointer to slot that points to an entry that matches the key and
  // hash.  If there is no such entry, return a pointer to the trailing slot
  // in the corresponding linked list.
  E** FindPointer(const Slice& key, uint32_t hash) const {
    E** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  // Insert entry to a given slot.  If the slot points to an existing entry, the
  // entry is removed and returned to the caller. Otherwise, NULL is returned.
  E* Inject(E* e, E** ptr) {
    E* old = *ptr;
    e->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = e;
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // We aim for a small average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  // Add a new entry to the hash table.  If an entry with the same key and
  // hash exists, it will be removed and returned to the caller.
  // Otherwise, NULL is returned.
  E* Insert(E* e) {
    E** const ptr = FindPointer(e->key(), e->hash);
    return Inject(e, ptr);
  }

  // Return the removed entry if one exists, NULL otherwise.
  E* Remove(const Slice& key, uint32_t hash) {
    E** ptr = FindPointer(key, hash);
    E* e = *ptr;
    if (e != NULL) {
      *ptr = e->next_hash;
      --elems_;
    }
    return e;
  }

  bool Empty() const { return elems_ == 0; }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;  // Total number of elements
  E** list_;

  // No copying allowed
  void operator=(const HashTable& hashtable);
  HashTable(const HashTable&);

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    E** new_list = new E*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      E* e = list_[i];
      while (e != NULL) {
        E* next = e->next_hash;
        uint32_t hash = e->hash;
        E** ptr = &new_list[hash & (new_length - 1)];
        e->next_hash = *ptr;
        *ptr = e;
        e = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// All values stored in the table are weak referenced and are owned by external
// entities. Removing values from the table or deleting the table itself will
// not release the memory of those values. This data structure requires external
// synchronization when accessed by multiple threads.
template <typename T = void>
class HashMap {
 private:
  typedef HashEntry<T> E;
  // Dummy head of list: list_.prev is the last entry,
  // list_.next is the first entry.
  E list_;

  HashTable<E> table_;

  void operator=(const HashMap& hashmap);  // No copying allowed
  HashMap(const HashMap&);

  void Remove(E* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
  }

  void Append(E* e) {
    e->next = &list_;
    e->prev = list_.prev;
    e->prev->next = e;
    e->next->prev = e;
  }

  static uint32_t hashval(const Slice& in) {
    return Hash(in.data(), in.size(), 0);
  }

 public:
  HashMap() {
    // Make empty circular linked list
    list_.next = &list_;
    list_.prev = &list_;
  }

  ~HashMap() {
    for (E* e = list_.next; e != &list_;) {
      E* next = e->next;
      free(e);
      e = next;
    }
  }

  bool Empty() const {
    return (list_.next == &list_) && (list_.prev == &list_);
  }

  class Visitor {
   public:
    virtual void visit(const Slice& k, T* v) = 0;
    virtual ~Visitor() {}
  };
  void VisitAll(Visitor* v) const {
    for (E* e = list_.next; e != &list_; e = e->next) {
      v->visit(e->key(), e->value);
    }
  }

  T* Lookup(const Slice& key) const {
    E* e = *table_.FindPointer(key, hashval(key));
    if (e != NULL) {
      return e->value;
    } else {
      return NULL;
    }
  }

  T* Insert(const Slice& key, T* value) {
    const size_t base = sizeof(E);
    E* e = static_cast<E*>(malloc(base - 1 + key.size()));
    e->value = value;
    e->key_length = key.size();
    e->hash = hashval(key);
    memcpy(e->key_data, key.data(), key.size());
    Append(e);

    T* old_value = NULL;
    E* old = table_.Insert(e);
    if (old != NULL) {
      Remove(old);
      old_value = old->value;
      free(old);
    }
    return old_value;
  }

  bool Contains(const Slice& key) const {
    return *table_.FindPointer(key, hashval(key)) != NULL;
  }

  T* Erase(const Slice& key) {
    T* value = NULL;
    E* e = table_.Remove(key, hashval(key));
    if (e != NULL) {
      Remove(e);
      value = e->value;
      free(e);
    }
    return value;
  }
};

// This data structure requires external synchronization when accessed by
// multiple threads.
class HashSet {
 public:
  void Erase(const Slice& key) { map_.Erase(key); }
  void Insert(const Slice& key) { map_.Insert(key, NULL); }
  bool Contains(const Slice& key) { return map_.Contains(key); }
  bool Empty() const { return map_.Empty(); }

  class Visitor {
   public:
    virtual void visit(const Slice& k) = 0;
    virtual ~Visitor() {}
  };
  void VisitAll(Visitor* v) const {
    struct Adaptor : public HashMap<>::Visitor {
      HashSet::Visitor* v;
      virtual void visit(const Slice& key, void* value) {
        assert(value == NULL);
        v->visit(key);
      }
    };

    Adaptor ada;
    ada.v = v;
    map_.VisitAll(&ada);
  }

  HashSet() {}

 private:
  // No copying allowed
  void operator=(const HashSet& hashset);
  HashSet(const HashSet&);

  HashMap<> map_;
};

}  // namespace pdlfs
