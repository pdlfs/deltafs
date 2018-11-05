/*
 * Copyright (c) 2015-2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "deltafs_plfsio_types.h"

namespace pdlfs {
namespace plfsio {

// Deltafs Plfs Dir Writer
class DirWriter {
 public:
  ~DirWriter();

  // Report the I/O stats for logging the data and the indexes.
  IoStats TEST_iostats() const;

  // Return the estimated size of each SSTable.
  // The actual memory, and storage, used by each sst may differ.
  uint64_t TEST_estimated_sstable_size() const;

  // Return the planned size of each filter.
  // This is the amount of memory we reserve for the filter associated with each
  // table. The actual memory, and storage, used by each filter may differ.
  uint64_t TEST_planned_filter_size() const;

  // Return the total number of keys inserted so far.
  uint32_t TEST_num_keys() const;

  // Return the total number of keys rejected so far.
  uint32_t TEST_num_dropped_keys() const;

  // Return the total number of data blocks generated so far.
  uint32_t TEST_num_data_blocks() const;

  // Return the total number of SSTable generated so far.
  uint32_t TEST_num_sstables() const;

  // Return the aggregated size of all index blocks.
  // Before compression and excluding any padding or checksum bytes.
  uint64_t TEST_raw_index_contents() const;

  // Return the aggregated size of all filter blocks.
  // Before compression and excluding any padding or checksum bytes.
  uint64_t TEST_raw_filter_contents() const;

  // Return the aggregated size of all data blocks.
  // Excluding any padding or checksum bytes.
  uint64_t TEST_raw_data_contents() const;

  // Return the aggregated size of all inserted keys.
  uint64_t TEST_key_bytes() const;

  // Return the aggregated size of all inserted values.
  uint64_t TEST_value_bytes() const;

  // Return the total amount of memory reserved by this directory.
  uint64_t TEST_total_memory_usage() const;

  // Open an I/O writer against a specified plfs-style directory.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const DirOptions& options, const std::string& dirname,
                     DirWriter** result);

  // Append a piece of data to a specific file under the directory.
  // Set epoch to -1 to disable epoch validation.
  // REQUIRES: Finish() has not been called.
  Status Add(const Slice& fid, const Slice& data, int epoch = -1);

  // Force a memtable compaction.
  // Set epoch to -1 to disable epoch validation.
  // REQUIRES: Finish() has not been called.
  Status Flush(int epoch = -1);

  // Force a memtable compaction and start a new epoch.
  // Set epoch to -1 to disable epoch validation.
  // REQUIRES: Finish() has not been called.
  Status EpochFlush(int epoch = -1);

  // Wait for all on-going background compactions to finish.
  // Return OK on success, or a non-OK status on errors.
  Status Wait();

  // Sync storage I/O.
  // Return OK on success, or a non-OK status on errors.
  Status Sync();

  // Force a compaction and finalize all log files.
  // No further write operation is allowed after this call.
  Status Finish();

 private:
  struct Rep;

  static Status TryOpen(Rep* rep);
  explicit DirWriter(Rep* rep);
  void operator=(const DirWriter& dw);
  DirWriter(const DirWriter&);

  Rep* rep_;
};

// Deltafs Plfs Dir Reader
class DirReader {
 public:
  DirReader() {}
  virtual ~DirReader();

  // Open an I/O reader against a specific plfs-style directory.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const DirOptions& options, const std::string& dirname,
                     DirReader** result);

  // Default: check all epochs.
  struct CountOp {
    CountOp();
    void SetEpoch(int epoch);
    uint32_t epoch_start;
    uint32_t epoch_end;
  };
  // Obtain the total num of keys stored in a given epoch range.
  // Return OK on success, or a non-OK status on errors.
  virtual Status Count(const CountOp& op, size_t* result) = 0;

  // Default: fetch all epochs and allow parallel reads
  struct ReadOp {
    ReadOp();
    void SetEpoch(int epoch);
    uint32_t epoch_start;
    uint32_t epoch_end;
    bool no_parallel_reads;
    size_t* table_seeks;
    size_t* seeks;
  };
  // Obtain the value to a specific key stored in a given epoch range.
  // Report operation stats in *table_seeks and *seeks.
  // Return OK on success, or a non-OK status on errors.
  virtual Status Read(const ReadOp& op, const Slice& fid, std::string* dst) = 0;

  // Default: scan all epochs and allow parallel reads
  struct ScanOp {
    ScanOp();
    void SetEpoch(int epoch);
    uint32_t epoch_start;
    uint32_t epoch_end;
    bool no_parallel_reads;
    size_t* table_seeks;
    size_t* seeks;
    size_t* n;
  };
  typedef int (*ScanSaver)(void* arg, const Slice& key, const Slice& value);
  // List all keys stored in a given epoch range.
  // Report operation stats in *table_seeks, *seeks, and *n.
  // Return OK on success, or a non-OK status on errors.
  virtual Status Scan(const ScanOp& op, ScanSaver, void*) = 0;

  // Return the aggregated I/O stats accumulated so far.
  virtual IoStats TEST_iostats() const = 0;

 private:
  // No copying allowed
  void operator=(const DirReader&);
  DirReader(const DirReader&);
};

}  // namespace plfsio
}  // namespace pdlfs
