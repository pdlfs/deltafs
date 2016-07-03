#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/dcntl.h"
#include "pdlfs-common/lru.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

struct Lease;
struct LeaseEntry;
struct LeaseOptions {
  LeaseOptions();
  uint64_t max_lease_duration;
  size_t max_num_leases;
};

// Lease states
// ------------
// a) If kFreeState, the lease is not being shared by any client;
// b) If kReadState, the lease may be shared among multiple clients and
// each incoming lookup request may extend the expiration time of the lease;
// a lease in read state but with a due in the past is considered free
// because all clients at the moment must have already discarded the lease;
// c) If kWriteState, the lease may be shared among multiple clients and
// there is an on-going write operation that modifies the contents of the lease;
// the write operation must wait until the lease expires before applying and
// publishing any changes; each lookup request must not further extend the
// expiration time of the lease but may choose to wait until that write
// operation finishes so a new expiration time may be set.
enum LeaseState { kFreeState, kReadState, kWriteState };

class LeaseTable;

struct Lease {
  typedef LeaseEntry Ref;
  typedef RefGuard<LeaseTable, Ref> Guard;
  bool busy() const;
#if defined(DELTAFS)
  explicit Lease() {}
  uint64_t seq;
#endif
#if defined(INDEXFS)
  Lease(port::Mutex* mu) : cv(mu) {}
  port::CondVar cv;
  uint64_t ino;
  uint32_t mode;
  uint32_t uid;
  uint32_t gid;
  uint32_t zeroth_server;
#endif
  const Dir* parent;
  uint64_t due;
  LeaseState state;
};

struct LeaseEntry {
  Lease* value;
  void (*deleter)(const Slice&, Lease* value);
  LeaseEntry* next_hash;
  LeaseEntry* next;
  LeaseEntry* prev;
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

// An LRU-cache of directory lookup state leases.
class LeaseTable {
 public:
  // If mu is NULL, this LeaseTable requires external synchronization.
  // If mu is not NULL, this LeaseTable is implicitly synchronized via this
  // mutex and is thread-safe.
  explicit LeaseTable(const LeaseOptions&, port::Mutex* mu = NULL);
  ~LeaseTable();

  void Release(Lease::Ref* ref);
  Lease::Ref* Lookup(const DirId& pid, const Slice& nhash);
  Lease::Ref* Insert(const DirId& pid, const Slice& nhash, Lease* lease);
  void Erase(const DirId& pid, const Slice& nhash);

 private:
  static Slice LRUKey(const DirId&, const Slice&, char* scratch);
  LeaseOptions options_;
  LRUCache<Lease::Ref> lru_;
  port::Mutex* mu_;

  // No copying allowed
  void operator=(const LeaseTable&);
  LeaseTable(const LeaseTable&);
};

}  // namespace pdlfs
