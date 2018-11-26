/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/cache.h"
#include "pdlfs-common/lru.h"
#include "pdlfs-common/mutexlock.h"

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
