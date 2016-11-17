#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <string>
#include <vector>

#include "deltafs_plfsio_api.h"
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

// Log data as multiple sorted runs of tables. Implementation is thread-safe.
class IOLogger {
 public:
  IOLogger(const Options& options, port::Mutex* mu, port::CondVar* cv,
           LogSink* data, LogSink* index);
  ~IOLogger();

  // REQUIRES: mutex_ has been locked
  Status Add(const Slice& key, const Slice& value);
  Status MakeEpoch(bool dry_run);
  Status Finish(bool dry_run);

 private:
  // No copying allowed
  void operator=(const IOLogger&);
  IOLogger(const IOLogger&);

  static void BGWork(void*);
  void MaybeSchedualCompaction();
  Status Prepare(bool force_epoch, bool do_finish);
  void CompactWriteBuffer();

  const Options& options_;
  port::Mutex* const mutex_;
  port::CondVar* const bg_cv_;
  // State below is protected by mutex_
  bool has_bg_compaction_;
  bool pending_epoch_flush_;
  bool pending_finish_;
  bool bg_do_epoch_flush_;
  bool bg_do_finish_;
  TableLogger table_logger_;
  WriteBuffer* mem_buf_;
  WriteBuffer* imm_buf_;
  WriteBuffer buf0_;
  WriteBuffer buf1_;
};

}  // namespace plfsio
}  // namespace pdlfs
