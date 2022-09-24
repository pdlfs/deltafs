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

#include "pdlfs-common/fsdb0.h"
#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/lru.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

// An LRU-cache of directory indices.
class IndexCache {
  typedef LRUEntry<DirIndex> IndexEntry;

 public:
  // If mu is NULL, the resulting IndexCache requires external synchronization.
  // If mu is not NULL, the resulting IndexCache is implicitly synchronized
  // via it and is thread-safe.
  explicit IndexCache(size_t capacity = 4096, port::Mutex* mu = NULL);
  ~IndexCache();

  struct Handle {};
  void Release(Handle* handle);
  const DirIndex* Value(Handle* handle);

  Handle* Lookup(const DirId& id);
  Handle* Insert(const DirId& id, DirIndex* index);
  void Erase(const DirId& id);

 private:
  static Slice LRUKey(const DirId&, char* scratch);
  LRUCache<IndexEntry> lru_;
  port::Mutex* mu_;

  // No copying allowed
  void operator=(const IndexCache&);
  IndexCache(const IndexCache&);
};

}  // namespace pdlfs
