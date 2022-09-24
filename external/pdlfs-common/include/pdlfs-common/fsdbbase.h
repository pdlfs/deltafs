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

namespace pdlfs {

enum KeyType {
  kDirEntType = 1,      // File or directory entry with embedded inode
  kDirIdxType = 2,      // GIGA+ directory index (deltafs, indexfs only)
  kDirMetaType = 3,     // Directory partition metadata (deltafs, indexfs only)
  kSuperBlockType = 4,  // File system superblock
  kDataDesType = 5,     // Header block of stored files
  kDataBlockType = 6,   // Data block of stored files
  kInoType = 7  // Dedicated inode entry that can be hard linked (tablefs only)
};

// Keys for naming metadata stored in KV-stores. Each key has a type. In
// general, a key consists of a prefix and a suffix. The prefix encodes parent
// directory information and the type of the key. The suffix is typically either
// a filename or a hash of it.
class Key {
 public:
  Key() {}  // Intentionally not initialized for performance.
  // Initialize a key using a given prefix encoding.
  explicit Key(const Slice& prefix);
#if defined(DELTAFS_PROTO)
  Key(uint64_t dno, uint64_t ino, KeyType type);
#endif
#if defined(DELTAFS)
  Key(uint64_t reg, uint64_t snap, uint64_t ino, KeyType type);
  Key(uint64_t snap, uint64_t ino, KeyType type);
#endif
  // Directly initialize a key using a parent directory inode no.
  // and a specific key type.
  Key(uint64_t ino, KeyType type);
  // Quickly initialize a key using information
  // provided by a LookupStat or Stat.
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || \
    defined(INDEXFS)  // Tablefs does not use LookupStat
  Key(const LookupStat& stat, KeyType type);
#endif
  Key(const Stat& stat, KeyType type);

  void SetType(KeyType type);  // Reset the type of a key.
  // The following functions reset the suffix of
  // a key in different ways.
  void SetSuffix(const Slice& suff);
  void SetOffset(uint64_t off);

  // Other ways for setting suffixes.
  void SetName(const Slice& name);  // Names may be stored as hashes
#if defined(DELTAFS) || defined(INDEXFS)
  void SetHash(const Slice& hash);
#endif

  Slice prefix() const;  // Return the prefix encoding of a key in its entirety.
  // Return the type of a key.
  KeyType type() const;
  // Return the parent inode no. of a key.
  uint64_t inode() const;
#if defined(DELTAFS_PROTO)
  uint64_t dnode() const;
#endif
#if defined(DELTAFS)  // Return reg id and snap id individually.
  uint64_t snap_id() const;
  uint64_t reg_id() const;
#endif

  // Return the suffix encoding of a key in its entirety.
  Slice suffix() const;
  // Return suffix as an integer.
  uint64_t offset() const;
#if defined(DELTAFS) || defined(INDEXFS)
  Slice hash() const;
#endif

  // Return the final encoding of key.
  Slice Encode() const { return Slice(data(), size()); }

  // Return the raw bytes of a key. Indexfs uses immediate buf spaces.
#if defined(DELTAFS) || defined(INDEXFS)
  size_t size() const { return size_; }
  const char* data() const { return rep_; }
  char* data() { return rep_; }

  // Deltafs and tablefs use std::string.
#else
  size_t size() const { return rep_.size(); }
  const char* data() const { return &rep_[0]; }
  char* data() { return &rep_[0]; }
#endif

 private:
  // Indexfs keys have a small fixed length. So we use an immediate buf space.
#if defined(DELTAFS) || defined(INDEXFS)
  // Intentionally copyable
  char rep_[50];
  size_t size_;

  // Deltafs and tablefs store intact filenames in keys.
  // Filenames can be lengthy. So we use std::string.
#else
  std::string rep_;
#endif
};

struct DirInfo;
struct DirId {
  DirId() {}  // Intentionally not initialized for performance.
#if defined(DELTAFS_PROTO)
  DirId(uint64_t dno, uint64_t ino) : dno(dno), ino(ino) {}
#endif
#if defined(DELTAFS)
  DirId(uint64_t reg, uint64_t snap, uint64_t ino)
      : reg(reg), snap(snap), ino(ino) {}
#endif
  explicit DirId(uint64_t ino);  // Direct initialization via inodes.
  // Initialization via LookupStat or Stat.
#if defined(DELTAFS_PROTO) || defined(DELTAFS) || \
    defined(INDEXFS)  // Tablefs does not use LookupStat.
  explicit DirId(const LookupStat& stat);
#endif
  explicit DirId(const Stat& stat);

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "other",
  //   == 0 iff "*this" == "other",
  //   >  0 iff "*this" >  "other"
  int compare(const DirId& other) const;
  std::string DebugString() const;

  // Deltafs requires extra fields.
#if defined(DELTAFS_PROTO)
  uint64_t dno;
#endif
#if defined(DELTAFS)
  uint64_t reg;
  uint64_t snap;
#endif

  uint64_t ino;
};

inline bool operator==(const DirId& x, const DirId& y) {
#if defined(DELTAFS_PROTO)
  if (x.dno != y.dno) {
    return false;
  }
#endif
#if defined(DELTAFS)
  if (x.reg != y.reg) return false;
  if (x.snap != y.snap) return false;
#endif

  return (x.ino == y.ino);
}

inline bool operator!=(const DirId& x, const DirId& y) {
  return !(x == y);  // Reuse operator==
}

}  // namespace pdlfs
