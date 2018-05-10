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

#include <set>

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

// Directory compaction stats
struct DirOutputStats {  // All final sizes include padding and block trailers
  DirOutputStats();

  uint32_t total_num_keys_;
  uint32_t total_num_dropped_keys_;
  uint32_t total_num_blocks_;
  uint32_t total_num_tables_;

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

// Directory builder interface.
class DirBuilder {
 public:
  DirBuilder(const DirOptions& options, DirOutputStats* stats);
  virtual ~DirBuilder();

  // Return a new builder according to the given options.
  static DirBuilder* Open(const DirOptions& options, DirOutputStats* stats,
                          LogSink* data, LogSink* indx);

  virtual void Add(const Slice& key, const Slice& value) = 0;

  // Finish building the current table. Optionally, a filter can be specified
  // that is associated with the table.
  // REQUIRES: Finish() has not been called.
  virtual void EndTable(const Slice& filter_contents,
                        ChunkType filter_type) = 0;

  // Force the start of a new epoch.
  // REQUIRES: Finish() has not been called.
  virtual void FinishEpoch(uint32_t epoch) = 0;

  // Finalize directory contents.
  // No further writes.
  virtual void Finish(uint32_t epoch) = 0;

  // Report memory usage.
  virtual size_t memory_usage() const = 0;

 protected:
  friend class DirCompactor;
  const DirOptions& options_;
  DirOutputStats* const compac_stats_;
  Status status_;

  bool ok() const { return status_.ok(); }

  uint32_t num_entries_;  // Number of entries inserted within the current epoch
  uint32_t num_tabls_;    // Number of tables generated within the current epoch

  // Total number of epochs generated
  uint32_t num_eps_;

 private:
  // No copying allowed
  void operator=(const DirBuilder& db);
  DirBuilder(const DirBuilder&);
};

class SortedStringBlockBuilder;
class ArrayBlockBuilder;
class LogWriter;

// Write directory contents into an index log and a data log object. Directory
// contents are divided into epochs. Epoch id starts with 0, and increments
// sequentially. Data written into the directory is put into the current epoch
// until FinishEpoch() is called, which finalizes the current epoch and
// starts a new epoch. New data is then written into this new epoch.
template <typename T = SortedStringBlockBuilder>
class SeqDirBuilder : public DirBuilder {
 public:
  SeqDirBuilder(const DirOptions& options, DirOutputStats* stats, LogSink* data,
                LogSink* indx);
  virtual ~SeqDirBuilder();

  virtual void Add(const Slice& key, const Slice& value);

  // Force the start of a new table.
  // REQUIRES: Finish() has not been called.
  virtual void EndTable(const Slice& filter_contents, ChunkType filter_type);

  // Force the start of a new epoch.
  // REQUIRES: Finish() has not been called.
  virtual void FinishEpoch(uint32_t ep_seq);

  // Finalize table contents.
  // No further writes.
  virtual void Finish(uint32_t ep_seq);

  // Report memory usage.
  virtual size_t memory_usage() const;

 private:
  // End the current block and force the start of a new data block.
  // REQUIRES: Finish() has not been called.
  void EndBlock();

  // Flush buffered data blocks and finalize their indexes.
  // REQUIRES: Finish() has not been called.
  void Commit();
#ifndef NDEBUG
  // Used to verify the uniqueness of all input keys
  std::set<std::string> keys_;
#endif

  std::string smallest_key_;
  std::string largest_key_;
  std::string last_key_;
  uint32_t num_uncommitted_indx_;  // Number of uncommitted index entries
  uint32_t num_uncommitted_data_;  // Number of uncommitted data blocks
  bool pending_restart_;           // Request to restart the data block buffer
  bool pending_commit_;  // Request to commit buffered data and indexes
  size_t block_threshold_;
  T* data_block_;
  BlockBuilder indx_block_;  // Locate the data blocks within a table
  BlockBuilder epok_block_;  // Locate the tables within an epoch
  BlockBuilder root_block_;  // Locate each epoch
  bool pending_indx_entry_;
  BlockHandle last_data_info_;
  bool pending_meta_entry_;
  TableHandle last_tabl_info_;
  bool pending_root_entry_;
  EpochHandle last_epok_info_;
  std::string uncommitted_indexes_;
  uint64_t pending_data_flush_;  // Offset of the data pending flush
  uint64_t pending_indx_flush_;  // Offset of the index pending flush
  LogSink* data_sink_;
  uint64_t data_offset_;  // Latest data offset
  LogWriter* indx_writter_;
  LogSink* indx_sink_;
  bool finished_;
};

}  // namespace plfsio
}  // namespace pdlfs
