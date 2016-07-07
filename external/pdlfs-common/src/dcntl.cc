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

#include "logging.h"
#include "pdlfs-common/coding.h"
#include "pdlfs-common/dcntl.h"

namespace pdlfs {

bool DirInfo::DecodeFrom(Slice* input) {
  uint64_t tmp64;
  uint32_t tmp32;
  if (!GetVarint64(input, &tmp64) || !GetVarint32(input, &tmp32)) {
    return false;
  } else {
    mtime = tmp64;
    size = tmp32;
    assert(size >= 0);
    return true;
  }
}

Slice DirInfo::EncodeTo(char* scratch) const {
  char* p = scratch;
  p = EncodeVarint64(p, mtime);
  assert(size >= 0);
  p = EncodeVarint32(p, size);
  return Slice(scratch, p - scratch);
}

bool Dir::busy() const {
#if defined(DELTAFS)
  if (tx.NoBarrier_Load() != NULL) {
    return true;
  }
#endif
  if (locked) {
    return true;
  } else {
    assert(num_leases >= 0);
    return num_leases != 0;
  }
}

bool DirEntry::is_pinned() const { return value->busy(); }

DirTable::~DirTable() {
#ifndef NDEBUG
  lru_.Prune();
  assert(lru_.Empty());
#endif
}

DirTable::DirTable(size_t capacity, port::Mutex* mu)
    : lru_(capacity), mu_(mu) {}

void DirTable::Release(Dir::Ref* ref) {
  if (mu_ != NULL) {
    mu_->Lock();
  }
  lru_.Release(ref);
  if (mu_ != NULL) {
    mu_->Unlock();
  }
}

Slice DirTable::LRUKey(const DirId& id, char* scratch) {
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

Dir::Ref* DirTable::Lookup(const DirId& id) {
  char tmp[30];
  Slice key = LRUKey(id, tmp);
  uint32_t hash = Hash(key.data(), key.size(), 0);

  if (mu_ != NULL) {
    mu_->Lock();
  }
  Dir::Ref* r = lru_.Lookup(key, hash);
  if (mu_ != NULL) {
    mu_->Unlock();
  }
  return r;
}

static void DeleteDir(const Slice& key, Dir* dir) {
#if defined(DLOG)
  LOG_ASSERT(!dir->busy()) << "deleting active directory state!";
#else
  if (dir->busy()) {
    Log(Logger::Default(), "Fatal: deleting active directory state!");
    abort();
  }
#endif
  delete dir;
}

Dir::Ref* DirTable::Insert(const DirId& id, Dir* dir) {
  char tmp[30];
  Slice key = LRUKey(id, tmp);
  uint32_t hash = Hash(key.data(), key.size(), 0);

  if (mu_ != NULL) {
    mu_->Lock();
  }
  Dir::Ref* r;
  bool error = false;
  int err;
  if (lru_.Exists(key, hash)) {
    error = true;
    err = EEXIST;
  } else if (!lru_.Compact()) {
    error = true;
    err = ENOBUFS;
  } else {
    r = lru_.Insert(key, hash, dir, 1, DeleteDir);
  }
  if (mu_ != NULL) {
    mu_->Unlock();
  }
  if (error) {
    throw err;
  } else {
    return r;
  }
}

void DirTable::Erase(const DirId& id) {
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
