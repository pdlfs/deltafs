/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <errno.h>
#include <stddef.h>

#include "pdlfs-common/coding.h"
#include "pdlfs-common/lookup_cache.h"

namespace pdlfs {

static void (*Deleter)(const Slice&, LookupStat*) = LRUValueDeleter<LookupStat>;

LookupCache::~LookupCache() {
#ifndef NDEBUG
  lru_.Prune();
  assert(lru_.Empty());
#endif
}

LookupCache::LookupCache(size_t capacity, port::Mutex* mu)
    : lru_(capacity), mu_(mu) {}

void LookupCache::Release(Handle* handle) {
  if (mu_ != NULL) {
    mu_->Lock();
  }
  lru_.Release(reinterpret_cast<LookupEntry*>(handle));
  if (mu_ != NULL) {
    mu_->Unlock();
  }
}

LookupStat* LookupCache::Value(Handle* handle) {
  return reinterpret_cast<LookupEntry*>(handle)->value;
}

Slice LookupCache::LRUKey(const DirId& pid, const Slice& nhash, char* scratch) {
  char* p = scratch;
#if !defined(DELTAFS)
  EncodeFixed64(p, pid.ino);
  p += 8;
#else
  p = EncodeVarint64(p, pid.reg);
  p = EncodeVarint64(p, pid.snap);
  p = EncodeVarint64(p, pid.ino);
#endif
  memcpy(p, nhash.data(), nhash.size());
  return Slice(scratch, p - scratch + nhash.size());
}

LookupCache::Handle* LookupCache::Lookup(const DirId& pid, const Slice& nhash) {
  char tmp[50];
  Slice key = LRUKey(pid, nhash, tmp);
  uint32_t hash = Hash(key.data(), key.size(), 0);

  if (mu_ != NULL) {
    mu_->Lock();
  }
  Handle* h = reinterpret_cast<Handle*>(lru_.Lookup(key, hash));
  if (mu_ != NULL) {
    mu_->Unlock();
  }
  return h;
}

LookupCache::Handle* LookupCache::Insert(const DirId& pid, const Slice& nhash,
                                         LookupStat* stat) {
  char tmp[50];
  Slice key = LRUKey(pid, nhash, tmp);
  uint32_t hash = Hash(key.data(), key.size(), 0);

  if (mu_ != NULL) {
    mu_->Lock();
  }
  Handle* h =
      reinterpret_cast<Handle*>(lru_.Insert(key, hash, stat, 1, Deleter));
  if (mu_ != NULL) {
    mu_->Unlock();
  }
  return h;
}

void LookupCache::Erase(const DirId& pid, const Slice& nhash) {
  char tmp[50];
  Slice key = LRUKey(pid, nhash, tmp);
  uint32_t hash = Hash(key.data(), key.size(), 0);

  if (mu_ != NULL) {
    mu_->Lock();
  }
  lru_.Erase(key, hash);
  if (mu_ != NULL) {
    mu_->Unlock();
  }
}

}  // namespace pdlfs
