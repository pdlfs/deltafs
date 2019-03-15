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

#include "pdlfs-common/lease.h"
#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/status.h"

#include <assert.h>
#include <errno.h>

namespace pdlfs {

LeaseOptions::LeaseOptions()
    : max_lease_duration(1000 * 1000), max_num_leases(4096) {}

bool Lease::busy() const {
  if (state == kLeaseLocked) {
    return true;
  } else {
    return Env::Default()->NowMicros() < due;
  }
}

bool LeaseEntry::is_pinned() const { return value->busy(); }

LeaseTable::~LeaseTable() {
#ifndef NDEBUG
  // Wait for all leases to expire
  Env::Default()->SleepForMicroseconds(10 + options_.max_lease_duration);
  lru_.Prune();
  assert(lru_.Empty());
#endif
}

LeaseTable::LeaseTable(const LeaseOptions& options, port::Mutex* mu)
    : options_(options), lru_(options_.max_num_leases), mu_(mu) {}

void LeaseTable::Release(Lease::Ref* ref) {
  if (mu_ != NULL) {
    mu_->Lock();
  }
  lru_.Release(ref);
  if (mu_ != NULL) {
    mu_->Unlock();
  }
}

Slice LeaseTable::LRUKey(const DirId& pid, const Slice& nhash, char* scratch) {
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

Lease::Ref* LeaseTable::Lookup(const DirId& pid, const Slice& nhash) {
  char tmp[50];
  Slice key = LRUKey(pid, nhash, tmp);
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
  assert(!lease->busy());
  const Dir* parent = lease->parent;
  parent->num_leases--;
  assert(parent->num_leases >= 0);
  delete lease;
}

Lease::Ref* LeaseTable::Insert(const DirId& pid, const Slice& nhash,
                               Lease* lease) {
  char tmp[50];
  Slice key = LRUKey(pid, nhash, tmp);
  uint32_t hash = Hash(key.data(), key.size(), 0);

  if (mu_ != NULL) {
    mu_->Lock();
  }
  Lease::Ref* r = NULL;
  bool error = false;
  int err = 0;
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

void LeaseTable::Erase(const DirId& pid, const Slice& nhash) {
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
