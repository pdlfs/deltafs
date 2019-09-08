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
#include "pdlfs-common/cache.h"
#include "pdlfs-common/lru.h"
#include "pdlfs-common/mutexlock.h"

// This LRU cache implementation is primarily designed for the leveldb
// sub-component of the codebase. For a more general LRU cache implementation,
// consider using the LRUCache in "pdlfs-common/lru.h" directly.
//
// Levedb uses caches for two purposes. It caches SSTable data blocks and
// SSTable file handles (this includes the root index of each SSTable). Each
// SSTable data block typically consists of 4MB of data. An underlying
// filesystem open file descriptor is associated with each SSTable file handle.
// Caching prevents Leveldb from repeatedly reading the same data blocks and
// opening the same set of SSTable files. Both types of operation can be
// time-consuming.
//
// Known issue:
//
// Currently, leveldb code performs cache insertions by first looking up a key
// in a cache and then insertion is only performed when the key is not found in
// the cache. While each cache operation is protected by a mutex, the leveldb
// code uses no locking to ensure the atomicity of this lookup-and-insert
// operation.
//
// Thus, if two client threads perform cache insertion at about the same time,
// they can both lookup, find nothing, and both go do the costly operation
// (opening an SSTable file or reading an SSTable data block) and do the cache
// insertion. The second insertion would evict the first from the cache, leaving
// each thread having a handle on its own instance of the same data, and with
// only the second instance in the cache and both in memory (the first instance
// will be deleted from memory when the corresponding client thread unrefs its
// handle).
//
// We could change the cache insertion behavior to not replacing any existing
// entries, and return the current entry to the caller. But this complicates
// client code (since the caller will have to check special cases in which a
// cache handle is returned that is not on the caller's data). Moreover, this
// "no-replacing" semantic does not save clients from performing the "expensive"
// operation (reading an SSTable data block or opening an SSTable file).
//
// To ensure that "expensive" operations are performed only once by one client
// thread (this is not always necessary though), the client code will have to
// use locks outside of the cache. Thus only the first client thread does the
// expensive operation, and the other concurrent threads wait for the first to
// complete and reap its result.
//
// Unless keeping memory consumption low is critical, having multiple client
// threads perform the expensive operation and "compete" may lead to better
// performance.
namespace pdlfs {

Cache::~Cache() {}

class ShardedLRUCache : public Cache {
 private:
  port::Mutex id_mu_;
  uint64_t id_;  // The last allocated id number

  static inline uint32_t hashval(const Slice& in) {
    return Hash(in.data(), in.size(), 0);
  }

  static uint32_t sha(uint32_t hash) { return hash >> (32 - kNumShardBits); }

  enum { kNumShardBits = 4 };
  enum { kNumShards = 1 << kNumShardBits };

  typedef LRUEntry<> E;
  LRUCache<E> sh_[kNumShards];
  port::Mutex mu_[kNumShards];

 public:
  explicit ShardedLRUCache(size_t capacity) : id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      sh_[s].SetCapacity(per_shard);
    }
  }

  virtual ~ShardedLRUCache() {}

  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = hashval(key);
    const uint32_t s = sha(hash);
    MutexLock l(&mu_[s]);
    E* e = sh_[s].Insert(key, hash, value, charge, deleter);
    return reinterpret_cast<Handle*>(e);
  }

  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = hashval(key);
    const uint32_t s = sha(hash);
    MutexLock l(&mu_[s]);
    E* e = sh_[s].Lookup(key, hash);
    return reinterpret_cast<Handle*>(e);
  }

  virtual void Release(Handle* handle) {
    E* e = reinterpret_cast<E*>(handle);
    const uint32_t s = sha(e->hash);
    MutexLock l(&mu_[s]);
    sh_[s].Release(e);
  }

  virtual void Erase(const Slice& key) {
    const uint32_t hash = hashval(key);
    const uint32_t s = sha(hash);
    MutexLock l(&mu_[s]);
    sh_[s].Erase(key, hash);
  }

  virtual void* Value(Handle* handle) {
    E* e = reinterpret_cast<E*>(handle);
    return e->value;
  }

  virtual uint64_t NewId() {
    MutexLock l(&id_mu_);
    return ++(id_);
  }
};

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);  // Statically partitioned
}

}  // namespace pdlfs
