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
#pragma once

#include "pdlfs-common/fstypes.h"
#include "pdlfs-common/lru.h"
#include "pdlfs-common/mdb.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

// An LRU-cache of pathname lookup leases.
class LookupCache {
  typedef LRUEntry<LookupStat> LookupEntry;

 public:
  // If mu is NULL, the resulting LookupCache requires external synchronization.
  // If mu is not NULL, the resulting LookupCache is implicitly synchronized
  // via it and is thread-safe.
  explicit LookupCache(size_t capacity = 4096, port::Mutex* mu = NULL);
  ~LookupCache();

  struct Handle {};
  void Release(Handle* handle);
  LookupStat* Value(Handle* handle);

  Handle* Lookup(const DirId& pid, const Slice& nhash);
  Handle* Insert(const DirId& pid, const Slice& nhash, LookupStat* stat);
  void Erase(const DirId& pid, const Slice& nhash);

 private:
  static Slice LRUKey(const DirId&, const Slice&, char* scratch);
  LRUCache<LookupEntry> lru_;
  port::Mutex* mu_;

  // No copying allowed
  void operator=(const LookupCache&);
  LookupCache(const LookupCache&);
};

}  // namespace pdlfs
