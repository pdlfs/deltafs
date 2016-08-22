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
#include <string>

#include "pdlfs-common/cache.h"
#include "pdlfs-common/leveldb/db/dbformat.h"
#include "pdlfs-common/leveldb/iterator.h"
#include "pdlfs-common/leveldb/table.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

class Env;

// Thread-safe (provides internal synchronization)
class TableCache {
  typedef DBOptions Options;

 public:
  TableCache(const std::string& dbname, const Options* options, Cache* cache);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-NULL, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or NULL if no Table object underlies
  // the returned iterator.  The returned "*tableptr" object is owned by
  // the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, SequenceOff seq_off,
                        Table** tableptr = NULL);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, SequenceOff seq_off, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

 private:
  Status LoadTable(uint64_t file_number, uint64_t file_size, Table**,
                   RandomAccessFile**);

  // Load the table for the specified file number.  Bind the
  // given sequence offset to the table.
  Status FindTable(uint64_t file_number, uint64_t file_size,
                   SequenceOff seq_off, Cache::Handle**);

  // No copying allowed
  TableCache(const TableCache&);
  void operator=(const TableCache&);

  Env* const env_;
  const std::string dbname_;
  const Options* options_;
  Cache* cache_;
  uint64_t id_;
};

}  // namespace pdlfs
