/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <string>
#include <vector>

#include "deltafs_plfsio.h"
#include "deltafs_plfsio_format.h"

namespace pdlfs {
namespace plfsio {

// Non-thread-safe append-only in-memory table.
class WriteBuffer {
 public:
  explicit WriteBuffer() : num_entries_(0), finished_(false) {}
  ~WriteBuffer() {}

  void Reserve(uint32_t num_entries, size_t buffer_size);
  size_t CurrentBufferCapacity() const { return buffer_.capacity(); }
  size_t CurrentBufferSize() const { return buffer_.size(); }
  uint32_t NumEntries() const { return num_entries_; }
  void Add(const Slice& key, const Slice& value);
  Iterator* NewIterator() const;
  void FinishAndSort();
  void Reset();

 private:
  // Starting offsets of inserted entries
  std::vector<uint32_t> offsets_;
  std::string buffer_;
  uint32_t num_entries_;
  bool finished_;

  // No copying allowed
  void operator=(const WriteBuffer&);
  WriteBuffer(const WriteBuffer&);

  class Iter;
};

struct OutputStats {
  OutputStats();

  // Footer
  size_t footer_size;

  // Total size of data blocks
  size_t final_data_size;  // Including padding and block trailers
  size_t data_size;

  // Total size of the root meta block
  size_t final_meta_size;
  size_t meta_size;

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

// Write table contents into a set of log files.
class TableLogger {
 public:
  TableLogger(const DirOptions& options, LogSink* data, LogSink* indx);
  ~TableLogger();

  bool ok() const { return status_.ok(); }
  Status status() const { return status_; }

  void Add(const Slice& key, const Slice& value);
  Status Finish();

  // End the current block and force the start of a new data block.
  // REQUIRES: Finish() has not been called.
  void EndBlock();

  // Flush buffered data blocks and finalize their indexes.
  // REQUIRES: Finish() has not been called.
  void Commit();

  // Force the start of a new table.
  // Caller may optionally specify a corresponding filter block.
  // REQUIRES: Finish() has not been called.
  template <typename T>
  void EndTable(T* filter);

  // Force the start of a new epoch.
  // REQUIRES: Finish() has not been called.
  void MakeEpoch();

 private:
  const DirOptions& options_;
  OutputStats output_stats_;
  friend class DirLogger;

  // No copying allowed
  void operator=(const TableLogger&);
  TableLogger(const TableLogger&);

  Status status_;
  std::string smallest_key_;
  std::string largest_key_;
  std::string last_key_;
  uint32_t num_uncommitted_index_;  // Number of uncommitted index entries
  uint32_t num_uncommitted_data_;   // Number of uncommitted data blocks
  BlockBuilder data_block_;
  BlockBuilder index_block_;
  BlockBuilder meta_block_;
  bool pending_index_entry_;
  BlockHandle pending_index_handle_;
  bool pending_meta_entry_;
  TableHandle pending_meta_handle_;
  uint32_t num_tables_;  // Number of tables within an epoch
  uint32_t num_epochs_;  // Number of epochs generated
  std::string uncommitted_indexes_;
  LogSink* data_sink_;
  LogSink* indx_sink_;
  bool finished_;
};

// Sequentially format and write data as multiple sorted runs of indexed tables.
// Implementation is thread-safe and uses background threads.
class DirLogger {
 public:
  DirLogger(const DirOptions& options, port::Mutex* mu, port::CondVar* cv,
            LogSink* indx, LogSink* data, CompactionStats* stats);
  ~DirLogger();

  // REQUIRES: mutex_ has been locked
  Status Wait();  // Wait for all on-going compactions to finish
  // May trigger a new compaction
  Status Add(const Slice& key, const Slice& value);

  // Force a compaction and maybe wait for it
  struct FlushOptions {
    FlushOptions(bool epoch_flush = false, bool finalize = false);

    // Do not wait for compaction to finish
    // Default: true
    bool no_wait;
    // Status checks only
    // Default: false
    bool dry_run;
    // Force a new epoch
    // Default: false
    bool epoch_flush;
    // Finalize the directory
    // Default: false
    bool finalize;
  };
  Status Flush(const FlushOptions& options);

  // Pre-close all log files before de-referencing them
  Status PreClose();

 private:
  Status Prepare(bool force = false, bool epoch_flush = false,
                 bool finalize = false);

  // No copying allowed
  void operator=(const DirLogger&);
  DirLogger(const DirLogger&);

  static void BGWork(void*);
  void MaybeScheduleCompaction();
  void CompactMemtable();
  void DoCompaction();

  // Constant after construction
  const DirOptions& options_;
  port::CondVar* const bg_cv_;
  port::Mutex* const mu_;
  size_t bf_bits_;
  size_t bf_bytes_;          // Target bloom filter size
  uint32_t entries_per_tb_;  // Number of entries packed per table
  size_t tb_bytes_;          // Target table size

  // State below is protected by mutex_
  LogSink* data_;
  LogSink* indx_;
  CompactionStats* compaction_stats_;
  uint32_t num_flush_requested_;
  uint32_t num_flush_completed_;
  bool has_bg_compaction_;
  TableLogger table_logger_;
  void* filter_;  // void* since different types of filter might be used
  WriteBuffer* mem_buf_;
  WriteBuffer* imm_buf_;
  bool imm_buf_is_epoch_flush_;
  bool imm_buf_is_final_;
  WriteBuffer buf0_;
  WriteBuffer buf1_;
};

inline DirLogger::FlushOptions::FlushOptions(bool epoch_flush, bool finalize)
    : no_wait(true),
      dry_run(false),
      epoch_flush(epoch_flush),
      finalize(finalize) {}

// Retrieve table contents from a set of indexed log files.
class PlfsIoReader {
 public:
  PlfsIoReader(const DirOptions&, LogSource* data, LogSource* index);
  // Open a reader on top of a given set of log files.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const DirOptions& options, LogSource* data,
                     LogSource* index, PlfsIoReader** result);

  // Obtain the value to a key from all epoches.
  // All value found will be appended to "dst"
  // Return OK on success, or a non-OK status on errors.
  Status Gets(const Slice& key, std::string* dst);

  ~PlfsIoReader();

 private:
  typedef void (*Saver)(void* arg, const Slice& key, const Slice& value);

  // Obtain the value to a key from a specific block.
  // If key is found, "saver" will be called. Set *eok to true if keys
  // larger than the given one have been seen.
  // NOTE: "saver" may be called multiple times.
  // Return OK on success, or a non-OK status on errors.
  Status Get(const Slice& key, const BlockHandle& handle, Saver saver, void*,
             bool* eok /* end-of-key */);

  // Return true if the given key matches a specific filter block.
  bool KeyMayMatch(const Slice& key, const BlockHandle& handle);

  // Obtain the value to a key from a specific table.
  // If key is found, "saver" will be called.
  // NOTE: "saver" may be called multiple times.
  // Return OK on success, or a non-OK status on errors.
  Status Get(const Slice& key, const TableHandle& handle, Saver saver, void*);

  // Obtain the value to a key in a specific epoch.
  // If key is found, value is appended to "dst".
  // NOTE: a key may appear multiple times within a single epoch.
  // Return OK on success, and a non-OK status on errors.
  Status Get(const Slice& key, uint32_t epoch, std::string* dst);

  // No copying allowed
  void operator=(const PlfsIoReader&);
  PlfsIoReader(const PlfsIoReader&);
  // Constant after construction
  const DirOptions& options_;
  uint32_t num_epoches_;

  Iterator* epoch_iter_;
  Block* epoch_index_;
  LogSource* index_src_;
  LogSource* data_src_;
};

}  // namespace plfsio
}  // namespace pdlfs
