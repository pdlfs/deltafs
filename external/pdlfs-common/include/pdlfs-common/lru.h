/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#pragma once

#include "pdlfs-common/hash.h"
#include "pdlfs-common/hashmap.h"
#include "pdlfs-common/slice.h"

#include <stdlib.h>

// The following code implements an LRU cache of KV pairs. KV pairs are placed
// in a reference-counted handle. A user-supplied delete operation is invoked
// when a handle's reference count is decremented to zero. Both the cache and
// the clients using a handle hold a reference to the handle.
//
// A cache keeps two linked lists of KV pair handles in the cache. Each handle
// in the cache (i.e., referenced by the cache) is in *either* one of the two
// lists, but never both. KV handles still referenced by clients but erased (if
// removed by a client) or evicted (if removed by the cache itself) from the
// cache are in *neither* of the two lists.
//
// The lists are:
//
// * in-use: contains the KV handles currently referenced by clients and by
//     the cache itself, in no particular order. Items in this list are
//     effectively "pinned" in the cache and are not considered for eviction.
//
// * LRU: contains the KV handles currently only referenced by the cache but
//     not by any client, in LRU order. Items in this list are candidates
//     for eviction.
//
// KV handles are moved between these two lists by the Ref() and Unref()
// methods, when they detect an element in the cache acquiring or losing its
// first/last external reference.
namespace pdlfs {

// To manage KV pairs, we place KV pairs in handles that are opaque to clients.
// Each handle is represented by a cache entry defined below. Each entry is a
// variable length heap-allocated structure, and is kept simultaneously in a
// hash table (for fast access) and in a circular doubly linked list (for LRU
// ordering).
//
// Each cache entry has an "in_cache" boolean flag indicating whether the cache
// has a reference to it. The only ways that this flag can become "false"
// without the entry being passed to its "deleter" are a) via Erase(), b) via
// Insert() when a KV pair with a duplicate key is inserted, or c) on
// destruction of the cache (i.e., on ~LRUCache()).
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
  bool in_cache;  // True iff entry has a reference from the cache
  char key_data[1];  // Beginning of the key

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

// This base LRU cache implementation requires external synchronization.
template <typename E>
class LRUCache {
 private:
  // Max cache size.
  size_t capacity_;
  // Current capacity consumption.
  size_t usage_;

  // Dummy head of the "in-use" list.
  // Entries currently in use by clients, in addition to being referenced by the
  // cache itself. They all have "refs >= 2" and "in_cache == true".
  E in_use_;

  // Dummy head of the LRU list.
  // Entries currently only referenced by the cache but not by any client.
  // Only entries here are eligible for eviction.
  // lru_.prev is the newest entry; lru_.next is the oldest entry.
  E lru_;

  HashTable<E> table_;

  // No copying allowed
  void operator=(const LRUCache&);
  LRUCache(const LRUCache&);

  // Add a reference to a cache entry. Promote the entry to the "in_use_" list
  // when it gains its first external reference.
  void Ref(E* const e) {
    if (e->refs == 1 &&
        e->in_cache) {  // If on lru_ list, move to in_use_ list.
      LRU_Remove(e);
      LRU_Append(&in_use_, e);
    }
    e->refs++;
  }

  // Remove a reference from a cache entry. Demote the entry from the "in_use_"
  // list when it loses its last external reference. Delete the entry when it
  // loses its final reference.
  void Unref(E* const e) {
    assert(e->refs > 0);
    e->refs--;
    if (e->refs == 0) {  // Deallocate.
      assert(!e->in_cache);
      (*e->deleter)(e->key(), e->value);
      free(e);
    } else if (e->in_cache && e->refs == 1) {
      // No longer in use; move to the lru_ list.
      LRU_Remove(e);
      LRU_Append(&lru_, e);
    }
  }

  void LRU_Remove(E* const e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
  }

  void LRU_Append(E* list, E* const e) {
    // Make "e" newest entry by inserting just before *list
    e->next = list;
    e->prev = list->prev;
    e->prev->next = e;
    e->next->prev = e;
  }

  // Remove *e from the cache decreasing the reference count of it and reducing
  // cache usage.
  // REQUIRES: e is not NULL and *e must be in the cache.
  // REQUIRES: *e has been removed from table_.
  void Remove(E* const e) {
    assert(e && e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }

 public:
  LRUCache(size_t capacity = 0) : capacity_(capacity), usage_(0) {
    // Make empty circular linked lists
    in_use_.next = &in_use_;
    in_use_.prev = &in_use_;
    lru_.next = &lru_;
    lru_.prev = &lru_;
  }

  ~LRUCache() {
    assert(in_use_.next ==
           &in_use_);  // Error if caller has an unreleased handle
    for (E* e = lru_.next; e != &lru_;) {
      E* const next = e->next;
      assert(e->refs == 1);  // Invariant of the lru_ list
      assert(e->in_cache);
      // Remove from cache as if Remove() is called
      e->in_cache = false;
      Unref(e);
      e = next;
    }
  }

  void SetCapacity(size_t c) {
    // Separate from constructor so caller can easily
    // make an array of LRUCache
    capacity_ = c;
  }

  // Add a KV entry into the cache. If an entry with the same key is present in
  // the cache, the old entry will be kicked out as a side effect of the
  // insertion. After inserting the new entry, one or more entries in the lru_
  // list may be evicted to bring usage_ back below a specific threshold. In
  // extreme cases, the newly inserted entry will be evicted canceling the very
  // insertion of it we just performed.
  template <typename T>
  E* Insert(const Slice& key, uint32_t hash, T* value, size_t charge,
            void (*deleter)(const Slice& key, T* value)) {
    E* const e = static_cast<E*>(malloc(sizeof(E) - 1 + key.size()));
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->hash = hash;
    e->in_cache = false;
    e->refs = 1;  // This is for the handle to be returned to the client
    memcpy(e->key_data, key.data(), key.size());

    if (capacity_ > 0) {
      e->refs++;  // This is for the cache itself
      e->in_cache = true;
      LRU_Append(&in_use_, e);
      usage_ += charge;
      E* const old = table_.Insert(e);
      // Evicting the old entry from the cache if there is one. The old entry
      // must be in the cache because it was ejected from the hash table and all
      // entries in the hash table is in the cache. Because the old entry is in
      // the cache, we must remove it from cache.
      if (old) {
        Remove(old);
      }
    } else {  // Don't cache; caching is effectively turned off when !capacity_
      // next is read by key() in an assert; it must be set
      e->next = NULL;
    }
    while (usage_ > capacity_ && lru_.next != &lru_) {
      E* const old = lru_.next;  // This is the least recently used
      assert(old->refs == 1);
      E* const victim = table_.Remove(old->key(), old->hash);
      assert(old == victim);
      Remove(victim);
    }

    return e;
  }

  E* Lookup(const Slice& key, uint32_t hash) {
    E* const e = *table_.FindPointer(key, hash);
    if (e != NULL) {
      Ref(e);
    }
    return e;
  }

  void Prune() {
    while (lru_.next != &lru_) {
      E* const e = lru_.next;
      assert(e->refs == 1);
      E* const victim = table_.Remove(e->key(), e->hash);
      assert(e == victim);
      Remove(victim);
    }
  }

  void Erase(const Slice& key, uint32_t hash) {
    E* const e = table_.Remove(key, hash);
    if (e != NULL) {
      Remove(e);
    }
  }

  // Return True iff the cache is empty.
  bool Empty() const {
    return (in_use_.next == &in_use_) && (in_use_.prev == &in_use_) &&
           (lru_.next == &lru_) && (lru_.prev == &lru_);
  }

  // Return True if key is present in the cache. This operation does not change
  // the order of the entry in the cache.
  bool Exists(const Slice& key, uint32_t hash) const {
    return *table_.FindPointer(key, hash) != NULL;
  }

  void Release(E* e) {
    Unref(e);  // Must not use *e hereafter
  }
};

template <typename T>
void LRUValueDeleter(const Slice& key, T* value) {
  delete value;  // T is not of void type
}

}  // namespace pdlfs
