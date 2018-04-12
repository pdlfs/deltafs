/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "deltafs_plfsio.h"
#include "deltafs_plfsio_format.h"
#include "deltafs_plfsio_nio.h"

namespace pdlfs {
namespace plfsio {

// Return true iff directory keys are unique and ordered.
// In such cases, we always use binary search to lookup keys and each point
// lookup within an epoch or a table stops immediate once we find a match.
static inline bool IsKeyUniqueAndOrdered(DirMode mode) {
  return (mode & 0xF0) == 0x80;
}

// Return true iff directory keys are unique.
// In such cases, each point lookup within an epoch or a table stops immediate
// once we find a match. However, if keys are stored out-of-order, we will have
// to use linear search to find our first match. On the other hand, if keys are
// not unique, we will resort to linear search for point lookups and each point
// lookup within an epoch or a table will continue even if we already find one
// or more matches. In such cases, however, a point lookup is able to stop
// immediate once we come across a key that is greater than the target key as
// long as keys are stored in-order.
static inline bool IsKeyUnique(DirMode mode) { return (mode & 0x80) == 0x80; }

// Return true iff directory keys are stored out-of-order.
// In such cases, we always use linear search for point lookups. If keys are
// unique, each point lookup within an epoch or a table stops immediate once we
// find a match. Otherwise, such lookup has to continue even when multiple
// matches have been found.
static inline bool IsKeyUnOrdered(DirMode mode) {
  return (mode & 0x10) == 0x10;
}

// Open an iterator on top of a given data block. The returned the iterator
// should be deleted when no longer needed.
extern Iterator* OpenDirBlock  // Use options to determine block formats
    (const DirOptions& options, const BlockContents& contents);

// Stats for indexed directory data.
struct DirOutputStats {  // All final sizes include padding and block trailers
  DirOutputStats();

  // Total size of data blocks
  size_t final_data_size;
  size_t data_size;

  // Total size of meta index blocks and the root meta index block
  size_t final_meta_index_size;
  size_t meta_index_size;

  // Total size of index blocks
  size_t final_index_size;
  size_t index_size;

  // Total size of filter blocks
  size_t final_filter_size;
  size_t filter_size;

  // Total size of user data compacted
  size_t value_size;
  size_t key_size;
};

// Index streaming directory data and write the results into a pair of log
// files.
class DirIndexer {
 public:
  explicit DirIndexer(const DirOptions& options);
  virtual ~DirIndexer();

  // Return a new indexer according to the given options.
  static DirIndexer* Open(const DirOptions& options, LogSink* data,
                          LogSink* indx);

  virtual void Add(const Slice& key, const Slice& value) = 0;

  // Finish building the current table. Optionally, a filter can be specified
  // that is associated with the table.
  // REQUIRES: Finish() has not been called.
  virtual void EndTable(const Slice& filter_contents,
                        ChunkType filter_type) = 0;

  // Force the start of a new epoch.
  // REQUIRES: Finish() has not been called.
  virtual void MakeEpoch() = 0;

  // Finalize directory contents.
  // No further writes.
  virtual Status Finish() = 0;

 protected:
  const DirOptions& options_;
  // Bytes generated for indexes, filters, formatted data, etc
  DirOutputStats output_stats_;
  Status status_;

  bool ok() const { return status_.ok(); }
  Status status() const { return status_; }

  template <typename T>
  friend class DirBuilder;

  // Indexing counters
  uint32_t total_num_keys_;
  uint32_t total_num_dropped_keys_;
  uint32_t total_num_blocks_;
  uint32_t total_num_tables_;
  uint32_t num_tables_;  // Number of tables generated within the current epoch
  uint32_t num_epochs_;  // Total number of epochs generated

  virtual size_t memory_usage() const = 0;

 private:
  // No copying allowed
  void operator=(const DirIndexer&);
  DirIndexer(const DirIndexer&);
};

}  // namespace plfsio
}  // namespace pdlfs
