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

#include <assert.h>

#include "pdlfs-common/leveldb/db/dbformat.h"

namespace pdlfs {

struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0), seq_off(0) {}

  int refs;
  int allowed_seeks;  // Max seeks until compaction
  uint64_t number;
  // File size in bytes
  uint64_t file_size;
  // Sequence offset to be applied to the table
  SequenceOff seq_off;

  // Key range
  InternalKey smallest;
  InternalKey largest;

  Slice smallest_user_key() const {
    Slice r = smallest.user_key();
    return r;
  }

  Slice largest_user_key() const {
    Slice r = largest.user_key();
    return r;
  }
};

}  // namespace pdlfs
