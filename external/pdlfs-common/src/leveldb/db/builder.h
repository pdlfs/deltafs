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

/*
 * Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found at https://github.com/google/leveldb.
 */
#pragma once

#include "pdlfs-common/leveldb/internal_types.h"
#include "pdlfs-common/leveldb/options.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

class TableCache;
class Iterator;

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

// Build a Table file from the contents of *iter. The generated file will be
// named according to meta->number. On success, the rest of *meta will be
// filled with metadata about the generated table. If no data is present in
// *iter, meta->file_size will be set to zero, and no file will be produced.
extern Status BuildTable(  ///
    const std::string& dbname, Env* env, const DBOptions& options,
    TableCache* table_cache, Iterator* iter, SequenceNumber* min_seq,
    SequenceNumber* max_seq, FileMetaData* meta);

}  // namespace pdlfs
