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
#include "pdlfs-common/env.h"
#include "pdlfs-common/lease.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

bool Lease::busy() const {
  if (state == kWriteState) {
    return true;
  } else {
    return Env::Default()->NowMicros() < due;
  }
}

bool LeaseEntry::is_pinned() const { return value->busy(); }

LeaseTable::~LeaseTable() {
#ifndef NDEBUG
  // Wait for all leases to expire
  Env::Default()->SleepForMicroseconds(1000);
  lru_.Prune();
  assert(lru_.Empty());
#endif
}

LeaseTable::LeaseTable(size_t capacity, port::Mutex* mu)
    : lru_(capacity), mu_(mu) {}

void LeaseTable::Release(Lease::Ref* ref) {
  if (mu_ != NULL) {
    mu_->Lock();
  }
  lru_.Release(ref);
  if (mu_ != NULL) {
    mu_->Unlock();
  }
}

Slice LeaseTable::LRUKey(uint64_t parent, const Slice& nhash, char* scratch) {
  EncodeFixed64(scratch, parent);
  assert(nhash.size() == 8);
  memcpy(scratch + 8, nhash.data(), nhash.size());
  return Slice(scratch, 16);
}

Lease::Ref* LeaseTable::Lookup(uint64_t parent, const Slice& nhash) {
  char tmp[16];
  Slice key = LRUKey(parent, nhash, tmp);
  uint32_t hash = Hash(key.data(), key.size(), 0);

  if (mu_ != NULL) {
    mu_->Lock();
  }
  Lease::Ref* r = lru_.Lookup(key, hash);
  if (mu_ != NULL) {
    mu_->Unlock();
  }
  return r;
}

static void DeleteLease(const Slice& key, Lease* lease) {
#if defined(GLOG)
  LOG_ASSERT(!lease->busy()) << "deleting active lease state!";
#else
  if (lease->busy()) {
    fprintf(stderr, "Error: deleting active lease state!");
    abort();
  }
#endif
  const Dir* parent = lease->parent;
  parent->num_leases--;
  assert(parent->num_leases >= 0);
  delete lease;
}

Lease::Ref* LeaseTable::Insert(uint64_t parent, const Slice& nhash,
                               Lease* lease) {
  char tmp[16];
  Slice key = LRUKey(parent, nhash, tmp);
  uint32_t hash = Hash(key.data(), key.size(), 0);

  if (mu_ != NULL) {
    mu_->Lock();
  }
  Lease::Ref* r;
  bool error = false;
  int err;
  if (lru_.Exists(key, hash)) {
    error = true;
    err = EEXIST;
  } else if (!lru_.Compact()) {
    error = true;
    err = ENOBUFS;
  } else {
    r = lru_.Insert(key, hash, lease, 1, DeleteLease);
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

void LeaseTable::Erase(uint64_t parent, const Slice& nhash) {
  char tmp[16];
  Slice key = LRUKey(parent, nhash, tmp);
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
