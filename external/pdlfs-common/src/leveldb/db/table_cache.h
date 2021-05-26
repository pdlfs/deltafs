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
#include "pdlfs-common/leveldb/iterator.h"
#include "pdlfs-common/leveldb/table.h"

#include "pdlfs-common/cache.h"
#include "pdlfs-common/port.h"

#include <stdint.h>
#include <string>

namespace pdlfs {

class Env;

// Thread-safe (provides internal synchronization)
class TableCache {
  typedef DBOptions Options;

 public:
  TableCache(const std::string& dbname, const Options* options, Cache* cache);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding file
  // length must be exactly "file_size" bytes). If "tableptr" is non-NULL, also
  // sets "*tableptr" to point to the Table object underlying the returned
  // iterator, or NULL if no Table object underlies the returned iterator. The
  // returned "*tableptr" object is owned by the cache and should not be
  // deleted, and is valid for as long as the returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, SequenceOff seq_off,
                        Table** tableptr = NULL);
  // This one is similar to the one above except that it will bypass the cache.
  // In addition, if prefetch_table is true the entire table will be read into
  // memory absorbing subsequent random reads to the table.
  Iterator* NewDirectIterator(const ReadOptions& options, bool prefetch_table,
                              uint64_t file_number, uint64_t file_size,
                              SequenceOff seq_off, Table** tableptr = NULL);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, SequenceOff seq_off, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

 private:
  // Fetch table from storage. By default, only table header and metadata blocks
  // are fetched. If prefetch is true, will read the entire table into memory so
  // all subsequent table reads will hit the memory.
  Status OpenTable(uint64_t file_number, uint64_t file_size, Table** table,
                   RandomAccessFile** file, bool prefetch);

  // Find the table for the specified file number from cache. If table is not
  // yet cached, it will be loaded from storage and assigned the given sequence
  // offset.
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
