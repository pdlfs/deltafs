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

// Append-only in-memory table implementation.
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

// Stores the contents of a list of tables in a set of log files.
class TableLogger {
 public:
  TableLogger(const Options& options, LogSink* data, LogSink* index);

  ~TableLogger() {}

  void Add(const Slice& key, const Slice& value);
  void EndBlock();  // Force the start of a new data block
  void EndTable();  // Force the start of a new table
  void EndEpoch();  // Force the start of a new epoch

  Status Finish();

 private:
  // No copying allowed
  void operator=(const TableLogger&);
  TableLogger(const TableLogger&);

  bool ok() const { return status_.ok(); }

  Options options_;
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

// Deltafs plfs-style io api.
class Writer {
 public:
 private:
  // XXX: TODO
};

}  // namespace plfsio
}  // namespace pdlfs
