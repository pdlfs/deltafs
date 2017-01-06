#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/guard.h"
#include "pdlfs-common/lru.h"
#include "pdlfs-common/mdb.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

class DirTable;

struct Dir;
struct DirEntry;
struct DirInfo {
  DirInfo() {}
  bool DecodeFrom(Slice* input);
  Slice EncodeTo(char* scratch) const;
  uint64_t mtime;
  int size;
};

struct Dir {
  typedef DirEntry Ref;
  typedef RefGuard<DirTable, Ref> Guard;
  bool busy() const;
  Dir(port::Mutex* mu, const DirIndexOptions* o) : cv(mu), index(o) {}
  uint64_t mtime;  // Last modification time

#if defined(DELTAFS)
  uint64_t seq;  // Incremented whenever a sub-directory's lookup state changes
  port::AtomicPointer tx;  // Either NULL or an on-going write transaction
  class Tx;
#endif
  port::CondVar cv;
  DirIndex index;  // GIGA+ index
  Status status;
  mutable int num_leases;  // Total number of leases blow this directory
  int size;
  bool locked;

  void Lock() {
    while (locked) cv.Wait();
    locked = true;
  }

  void Unlock() {
    assert(locked);
    locked = false;
    cv.SignalAll();
  }
};

struct DirEntry {
  Dir* value;
  void (*deleter)(const Slice&, Dir* value);
  DirEntry* next_hash;
  DirEntry* next;
  DirEntry* prev;
  size_t charge;
  size_t key_length;
  uint32_t refs;
  uint32_t hash;  // Hash of key(); used for fast partitioning and comparisons
  char key_data[1];  // Beginning of key

  bool is_pinned() const;

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

#if defined(DELTAFS)
class Dir::Tx {
  MDB::Tx* const rep_;
  int refs_;
  void operator=(const Tx&);
  Tx(const Tx&);
  ~Tx() {}

 public:
  explicit Tx(MDB* mdb) : rep_(mdb->CreateTx()), refs_(0) {}
  MDB::Tx* rep() const { return rep_; }

  void Ref() { ++refs_; }
  bool Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ == 0) {
      return true;  // Last reference
    } else {
      return false;
    }
  }

  void Dispose(MDB* mdb) {
    assert(refs_ == 0);
    mdb->Release(rep_);
    delete this;
  }
};
#endif

// An LRU-cache of directory states.
class DirTable {
 public:
  // If mu is NULL, this DirTable requires external synchronization.
  // If mu is not NULL, this DirTable is implicitly synchronized via this
  // mutex and is thread-safe.
  explicit DirTable(size_t capacity = 4096, port::Mutex* mu = NULL);
  ~DirTable();

  void Release(Dir::Ref* ref);
  Dir::Ref* Lookup(const DirId& id);
  Dir::Ref* Insert(const DirId& id, Dir* dir);
  void Erase(const DirId& id);

 private:
  static Slice LRUKey(const DirId&, char* scratch);
  LRUCache<Dir::Ref> lru_;
  port::Mutex* mu_;

  // No copying allowed
  void operator=(const DirTable&);
  DirTable(const DirTable&);
};

}  // namespace pdlfs
