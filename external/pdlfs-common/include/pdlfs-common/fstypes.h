#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>

#include "pdlfs-common/slice.h"

namespace pdlfs {

enum KeyType {
  kDirEntType = 1,      // File or directory entry with embedded inode
  kDirIdxType = 2,      // Directory partition metadata (deltafs, indexfs only)
  kSuperBlockType = 3,  // File system superblock
  kDataBlockType = 4,   // Data block for files (tablefs only)
  kInodeType = 5  // Dedicated inode entry that can be hard linked(tablefs only)
};

class Key {
 public:
  Key() {}
  explicit Key(uint64_t dir_id, KeyType type);
  explicit Key(uint64_t dir_id, KeyType type, const Slice& name);
  void SetName(const Slice& name);
  void SetHash(const Slice& hash);

  uint64_t dir_id() const;
  KeyType type() const;
  Slice hash() const;
  Slice prefix() const;

  Slice Encode() const { return Slice(data(), size()); }
  size_t size() const { return sizeof(rep_); }
  const char* data() const { return rep_; }
  char* data() { return rep_; }

 private:
  // Intentionally copyable
  char rep_[16];
};

}  // namespace pdlfs
