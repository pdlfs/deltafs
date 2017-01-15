#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

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

  void Reserve(uint32_t num_entries, size_t size_per_entry);
  size_t CurrentBufferSize() const { return buffer_.size(); }
  uint32_t NumEntries() const { return num_entries_; }
  void Add(const Slice& key, const Slice& value);
  Iterator* NewIterator() const;
  void Finish();
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

// Write table contents into a set of log files.
class TableLogger {
 public:
  TableLogger(const Options& options, LogSink* data, LogSink* index);
  ~TableLogger();

  bool ok() const { return status_.ok(); }
  Status status() const { return status_; }

  void Add(const Slice& key, const Slice& value);
  void EndBlock();  // Force the start of a new data block
  void EndTable();  // Force the start of a new table
  void EndEpoch();  // Force the start of a new epoch

  Status Finish();

 private:
  enum { kDataBlkRestartInt = 16, kNonDataBlkRestartInt = 1 };

  // No copying allowed
  void operator=(const TableLogger&);
  TableLogger(const TableLogger&);

  const Options& options_;

  Status status_;
  std::string smallest_key_;
  std::string largest_key_;
  std::string last_key_;
  BlockBuilder data_block_;
  BlockBuilder index_block_;
  BlockBuilder epoch_block_;
  bool pending_index_entry_;
  BlockHandle pending_index_handle_;
  bool pending_epoch_entry_;
  TableHandle pending_epoch_handle_;
  uint32_t num_tables_;   // Number of tables within an epoch
  uint32_t num_epoches_;  // Number of epoches generated
  LogSink* data_log_;
  LogSink* index_log_;
  bool finished_;
};

// Sequentially log data as multiple sorted runs of indexed tables.
// Implementation is thread-safe and uses background threads.
class PlfsIoLogger {
 public:
  PlfsIoLogger(const Options& options, port::Mutex* mu, port::CondVar* cv,
               LogSink* data, LogSink* index);
  ~PlfsIoLogger();

  // REQUIRES: mutex_ has been locked
  Status Add(const Slice& key, const Slice& value);
  Status MakeEpoch(bool dry_run);
  Status Finish(bool dry_run);

 private:
  // No copying allowed
  void operator=(const PlfsIoLogger&);
  PlfsIoLogger(const PlfsIoLogger&);

  static void BGWork(void*);
  void MaybeSchedualCompaction();
  Status Prepare(bool do_epoch_flush, bool do_finish);
  void CompactWriteBuffer();
  void DoCompaction();

  // Constant after construction
  const Options& options_;
  port::Mutex* const mutex_;
  port::CondVar* const bg_cv_;
  uint32_t entries_per_table_;
  size_t table_size_;
  size_t bf_bytes_;
  size_t bf_bits_;

  // State below is protected by mutex_
  bool has_bg_compaction_;
  bool pending_epoch_flush_;
  bool pending_finish_;
  TableLogger table_logger_;
  WriteBuffer* mem_buf_;
  WriteBuffer* imm_buf_;
  bool imm_buf_is_epoch_flush_;
  bool imm_buf_is_finish_;
  WriteBuffer buf0_;
  WriteBuffer buf1_;
};

// Retrieve table contents from a set of indexed log files.
class PlfsIoReader {
 public:
  PlfsIoReader(const Options&, LogSource* data, LogSource* index);
  // Open a reader on top of a given set of log files.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const Options& options, LogSource* data, LogSource* index,
                     PlfsIoReader** result);

  // Obtain the value to a key from all epoches.
  // All value found will be appended to "dst"
  // Return OK on success, or a non-OK status on errors.
  Status Gets(const Slice& key, std::string* dst);

  ~PlfsIoReader();

 private:
  typedef void (*Saver)(void* arg, const Slice& key, const Slice& value);

  // Obtain the value to a key from a specific block.
  // If key is found, "saver" will be called.
  // NOTE: "saver" may be called multiple times.
  // Return OK on success, or a non-OK status on errors.
  Status Get(const Slice& key, const BlockHandle& handle, Saver saver, void*);

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
  const Options& options_;
  uint32_t num_epoches_;

  Iterator* epoch_iter_;
  Block* epoch_index_;
  LogSource* index_src_;
  LogSource* data_src_;
};

}  // namespace plfsio
}  // namespace pdlfs
