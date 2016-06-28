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
  if (tx != NULL) {
    return true;
  } else if (locked) {
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

Slice DirTable::LRUKey(uint64_t ino, char* scratch) {
  EncodeFixed64(scratch, ino);
  return Slice(scratch, 8);
}

Dir::Ref* DirTable::Lookup(uint64_t ino) {
  char tmp[8];
  Slice key = LRUKey(ino, tmp);
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
    fprintf(stderr, "Error: deleting active directory state!");
    abort();
  }
#endif
  delete dir;
}

Dir::Ref* DirTable::Insert(uint64_t ino, Dir* dir) {
  char tmp[8];
  Slice key = LRUKey(ino, tmp);
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

void DirTable::Erase(uint64_t ino) {
  char tmp[8];
  Slice key = LRUKey(ino, tmp);
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
