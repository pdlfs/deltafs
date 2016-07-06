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

#include "pdlfs-common/coding.h"
#include "pdlfs-common/index_cache.h"

namespace pdlfs {

static void (*Deleter)(const Slice&, DirIndex*) = LRUValueDeleter<DirIndex>;

IndexCache::~IndexCache() {
#ifndef NDEBUG
  lru_.Prune();
  assert(lru_.Empty());
#endif
}

IndexCache::IndexCache(size_t capacity, port::Mutex* mu)
    : lru_(capacity), mu_(mu) {}

void IndexCache::Release(Handle* handle) {
  if (mu_ != NULL) {
    mu_->Lock();
  }
  lru_.Release(reinterpret_cast<IndexEntry*>(handle));
  if (mu_ != NULL) {
    mu_->Unlock();
  }
}

DirIndex* IndexCache::Value(Handle* handle) {
  return reinterpret_cast<IndexEntry*>(handle)->value;
}

Slice IndexCache::LRUKey(const DirId& id, char* scratch) {
  char* p = scratch;
#if !defined(DELTAFS)
  EncodeFixed64(p, id.ino);
  p += 8;
#else
  p = EncodeVarint64(p, id.reg);
  p = EncodeVarint64(p, id.snap);
  p = EncodeVarint64(p, id.ino);
#endif
  return Slice(scratch, p - scratch);
}

IndexCache::Handle* IndexCache::Lookup(const DirId& id) {
  char tmp[30];
  Slice key = LRUKey(id, tmp);
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

IndexCache::Handle* IndexCache::Insert(const DirId& id, DirIndex* index) {
  char tmp[30];
  Slice key = LRUKey(id, tmp);
  uint32_t hash = Hash(key.data(), key.size(), 0);

  if (mu_ != NULL) {
    mu_->Lock();
  }
  Handle* h =
      reinterpret_cast<Handle*>(lru_.Insert(key, hash, index, 1, Deleter));
  if (mu_ != NULL) {
    mu_->Unlock();
  }
  return h;
}

void IndexCache::Erase(const DirId& id) {
  char tmp[30];
  Slice key = LRUKey(id, tmp);
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
