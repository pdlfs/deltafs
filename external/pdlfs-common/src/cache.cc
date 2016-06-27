#include "pdlfs-common/cache.h"

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/lru.h"
#include "pdlfs-common/mutexlock.h"

namespace pdlfs {

Cache::~Cache() {}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  typedef LRUEntry<> E;
  LRUCache<E> shard_[kNumShards];
  port::Mutex mu_[kNumShards];
  port::Mutex id_mu_;
  uint64_t id_;  // The last allocated id number

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }

  virtual ~ShardedLRUCache() {}

  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    const uint32_t s = Shard(hash);
    MutexLock l(&mu_[s]);
    E* e = shard_[s].Insert(key, hash, value, charge, deleter);
    return reinterpret_cast<Handle*>(e);
  }

  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    const uint32_t s = Shard(hash);
    MutexLock l(&mu_[s]);
    E* e = shard_[s].Lookup(key, hash);
    return reinterpret_cast<Handle*>(e);
  }

  virtual void Release(Handle* handle) {
    E* e = reinterpret_cast<E*>(handle);
    const uint32_t s = Shard(e->hash);
    MutexLock l(&mu_[s]);
    shard_[s].Release(e);
  }

  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    const uint32_t s = Shard(hash);
    MutexLock l(&mu_[s]);
    shard_[s].Erase(key, hash);
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
