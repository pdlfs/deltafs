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

#include <stdint.h>

// The following code implements an LRU cache of KV pairs. KV pairs are placed
// in a reference-counted handle. A user-supplied delete function is invoked
// when a handle's reference count is decremented to zero. Both the cache and
// the clients currently holding a handle keep a reference on the handle.
//
// A handle may be referenced only by the cache or only by clients. When a
// handle is only referenced by clients, it is no longer considered "in" the
// cache. A handle is removed from the cache (i.e., from "in" the cache to "out"
// of the cache) when it is evicted by the cache making room for new KV pairs.
// In addition, a client may also explicitly erase a key from a cache.
//
// Each cache keeps two linked lists of KV pair handles. Each live handle (i.e.,
// with one or more references) is in *either* one of the two lists, but
// *never* both. When a handle loses its last reference, it is removed from the
// list currently holding it and then sent to its "deleter".
//
// The two lists are:
//
// * in-use: contains the KV handles that are currently referenced by clients
//     and may or may not be referenced by the cache itself. Items in this list
//     are not kept in any particular order. In addition, items in this list
//     that are also "in" the cache are effectively "pinned" in the cache: they
//     are not considered for automatic eviction by the cache, though clients
//     are still able to erase such items from the cache explicitly. Once
//     erased, they may keep in the "in-use" list until they lose all their
//     external references.
//
// * LRU: contains the KV handles currently only referenced by the cache but
//     not by any client, in LRU order. Items in this list are considered "idle"
//     and candidates for eviction.
//
// KV handles currently "in" the cache are moved between the two lists when they
// acquire or lose their first or last external references. KV handles "out" of
// the cache may only be found in the first list. Note also that as long as a
// KV handle is still "in" the cache, a client may erase it from the cache
// regardless of which list is currently holding the handle.
//
// When KV handles are in the "LRU" list, they are subject to eviction during
// KV insertion and cache pruning operations. Because handles in the "LRU" list
// are only referenced by the cache, they are immediately deleted once evicted.
namespace pdlfs {

// To manage KV pairs, we place KV pairs in handles that are opaque to clients.
// Each handle is represented by a cache entry defined below. Each entry is a
// variable length heap-allocated structure, and is kept in a hash table (for
// fast lookups) as long as the entry is "in" the cache. Each entry is
// additionally put in a circular doubly linked list (either for LRU ordering
// or for sanity checks) as long as the entry is alive.
//
// Each cache entry has an "in_cache" boolean flag indicating whether it is "in"
// the cache. The only ways that this flag can become "False" without causing
// the entry to be passed to its "deleter" are a) via Erase()'ing an entry that
// still has external references, b) via Insert()'ing an entry that causes an
// existing entry with the same key that still has external references to be
// removed from the cache, or c) on destruction of the cache (i.e., on
// ~LRUCache()) when there are still entries with external references. The last
// case is considered problematic as it leaks resources permanently.
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
#if 0
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) return *(reinterpret_cast<Slice*>(value));
#endif
    return Slice(key_data, key_length);
  }
};

// This LRU cache implementation requires external synchronization. Two capacity
// usage numbers are maintained. One only counts entries currently "in" the
// cache. The other also considers entries not "in" the cache. These are entries
// only referenced by clients but not by the cache itself.
//
// * total_usage_: overall capacity consumption covering all cache entries
//     including those no longer "in" the cache. This usage number reflects
//     overall resource footprint for which the cache instance is not fully
//     responsible.
//
// * usage_: capacity consumption of entries "in" the cache. Cache is
//     responsible for keeping this below a specific threshold by evicting
//     "idle" entries in the cache. If inserting an entry would cause
//     usage to grow beyond a given threshold even after evicting all
//     "idle" entries, the insertion will be rejected such that the
//     returned cache handle is only referenced by the client but not by
//     the cache: the cache cannot be responsible for its usage.
//
// To keep *overall* capacity consumption below a certain limit, the client code
// must take action to achieve that rather than completely relying on the cache.
template <typename E>
class LRUCache {
 private:
  // Max cache size.
  size_t capacity_;
  // Capacity consumption also counting entries not "in" the cache.
  size_t total_usage_;
  // Current capacity consumption.
  size_t usage_;

  // Dummy head of the "in-use" list.
  // Entries currently in use by clients. They may or may not be referenced by
  // the cache itself (i.e., "in" the cache). They all have "refs >= 2" when
  // "in_cache == true". Otherwise, they have "refs >= 1".
  E in_use_;

  // Dummy head of the "LRU" list.
  // Entries currently only referenced by the cache but not by any client.
  // These are the only entries eligible for eviction. They all have "refs == 1"
  // and "in_cache == true". lru_.prev is the newest entry.
  // lru_.next is the oldest entry.
  E lru_;

  // In addition to one of the two lists above, each entry currently "in"
  // the cache is put here for fast lookups and presence checks.
  HashTable<E> table_;

  // No copying allowed
  void operator=(const LRUCache&);
  LRUCache(const LRUCache&);
  // Cache entries are created and inserted into the cache during Insert().
  // They are removed from the cache and deleted as follows:
  //
  // * This tier removes     Erase()  Insert()  Prune()
  //   entries from table_.         \    |     /
  //                                 \   |    /
  // * This tier handles              Remove()
  //   usage_ and in_cache_.             |     ~LRUCache()
  //                         Release()   |    /
  // * This tier handles              \  |   /
  //   lru_, in_use_, total_usage_,    \ |  /
  //   and deletion.                   Unref()
  //
  // Withdraw a reference from a given entry. Demote the entry from the
  // "in_use_" list when it is still "in" the cache but just loses its last
  // external reference. Delete the entry when it loses its final reference.
  // Note that we only maintain LRU order for entries in the "lru_" list.
  // Entries in the "in_use_" list are deemed "unordered".
  // REQUIRES: when *e is about to lose its final reference, it must have been
  // marked as removed from the cache (e->in_cache is False).
  void Unref(E* const e) {
    assert(e->refs > 0);
    e->refs--;
    if (e->refs == 0) {
      LRU_Remove(e);  // This can be either in_use_ or lru_
      total_usage_ -= e->charge;
      assert(!e->in_cache);
      (*e->deleter)(e->key(), e->value);
      free(e);
    } else if (e->in_cache && e->refs == 1) {
      // No longer in use; move to lru_
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

  // Remove *e from the cache decreasing its reference count and reducing cache
  // usage. Note that this function does not remove *e from table_. One must
  // first remove *e from table_ before calling this function.
  // REQUIRES: e is not NULL and e->in_cache is True.
  // REQUIRES: *e has been removed from table_.
  void Remove(E* const e) {
    assert(e && e->in_cache);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }

 public:
  // Setting capacity_ to 0 disables caching effectively
  explicit LRUCache(size_t capacity = 0)
      : capacity_(capacity), total_usage_(0), usage_(0) {
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
      assert(e->refs == 1);  // Invariants of the lru_ list
      assert(e->in_cache);
      // Mark *e as removed from cache as if
      // Remove() has been called
      e->in_cache = false;
      Unref(e);
      e = next;
    }
  }

  size_t total_usage() const {
    // Return the total usage of the cache.
    // Total usage includes entries both "in" and "out" of the cache.
    return total_usage_;
  }

  size_t usage() const {
    // Return the current usage of the cache
    return usage_;
  }

  size_t capacity() const {  // Return the configured capacity of the cache
    return capacity_;
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
  // extreme cases, the newly inserted entry will be ejected canceling the very
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
    LRU_Append(&in_use_, e);  // It has an outstanding reference from the client
    total_usage_ += charge;
    // Fast path for a special case in which
    // caching is effectively turned off via !capacity_.
    if (!capacity_) {
      return e;
    }
    e->refs++;  // This is for the cache itself
    e->in_cache = true;
    usage_ += charge;
    E* const old = table_.Insert(e);
    // Evicting the old entry from the cache if there is one.
    if (old) {
      Remove(old);
    }
    // Make room for the incoming entry.
    while (usage_ > capacity_ && lru_.next != &lru_) {
      E* const a = lru_.next;  // This is the least recently used
      assert(a->refs == 1);
      E* const victim = table_.Remove(a->key(), a->hash);
      assert(a == victim);
      Remove(victim);
    }
    // Don't cache the incoming entry if we turn out to have run out of room.
    if (usage_ > capacity_) {
      E* const victim = table_.Remove(key, hash);
      assert(e == victim);
      Remove(victim);
    }

    assert(usage_ <= capacity_);  // Invariant of the cache
    return e;
  }

  // Retrieve a key from the cache incrementing its reference count and moving
  // it to the "in_use_" list if it is not already there. Return NULL if the
  // specified key is not present in the cache. Entries in the "in_use_" list
  // won't be automatically evicted.
  E* Lookup(const Slice& key, uint32_t hash) {
    E* const e = *table_.FindPointer(key, hash);
    if (e != NULL) {
      Ref(e);
    }
    return e;
  }

  // Empty the "lru_" list reducing the cache's usage_.
  void Prune() {
    while (lru_.next != &lru_) {
      E* const e = lru_.next;
      assert(e->refs == 1);
      E* const victim = table_.Remove(e->key(), e->hash);
      assert(e == victim);
      Remove(victim);
    }
  }

  // Kick out a key from the cache decrementing its reference count and reducing
  // usage_. Erasing a key that is not in the cache has no effect. A key can be
  // erased as long as it is "in_cache" regardless if it is currently in the
  // "in_use_" list or in the "lru_" list. Once a key is erased, it will no
  // longer be Lookup()'d from the cache. If a key is erased from the "lru_"
  // list, it will be immediately deleted. Return the erased entry. Return NULL
  // if nothing has been erased.
  E* Erase(const Slice& key, uint32_t hash) {
    E* const e = table_.Remove(key, hash);
    if (e != NULL) {
      Remove(e);
    }
    return e;
  }

  // Erase an entry from the cache. No effect if the entry is not in the cache.
  // Return the entry if it has been removed. Return NULL otherwise.
  E* Erase(E* e) {
    e = table_.Remove(e);
    if (e != NULL) {
      Remove(e);
    }
    return e;
  }

  // Return True if the cache is empty. This does not count entries that are
  // "out" of the cache.
  bool Empty() const {  // An entry is "in" the cache if it is "in" table_
    return (table_.Empty());
  }

  // Return True if key is present in the cache. This operation does not change
  // the LRU order of the entry in the cache.
  bool Exists(const Slice& key, uint32_t hash) const {
    return *table_.FindPointer(key, hash) != NULL;
  }

  // Remove an external reference on a given entry potentially adjusting
  // its LRU order or causing it to be deleted.
  void Release(E* e) {
    // Must not use *e hereafter
    Unref(e);
  }

  // Add an external reference to a given entry. Promote the entry to the
  // "in_use_" list when it is "in" the cache and is about to gain its first
  // external reference. Note that we only maintain LRU order for entries in the
  // "lru_" list. Entries in the "in_use_" list are effectively regarded as
  // "unordered".
  void Ref(E* const e) {
    if (e->refs == 1 && e->in_cache) {
      // If *e is on lru_, move it to the "in_use_" list.
      LRU_Remove(e);
      LRU_Append(&in_use_, e);
    }
    e->refs++;
  }
};

template <typename T>
void LRUValueDeleter(const Slice& key, T* value) {
  delete value;  // T is not of void type
}

}  // namespace pdlfs
