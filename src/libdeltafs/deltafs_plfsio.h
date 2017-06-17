/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/env.h"
#include "pdlfs-common/port.h"

#include <stddef.h>
#include <stdint.h>

namespace pdlfs {
namespace plfsio {

struct CompactionStats {
  CompactionStats();

  // Total bytes written as indexes (including padding and checksums)
  uint64_t index_bytes;
  // Total number of write operations for indexes
  uint64_t index_ops;
  // Total bytes written as data (including padding and checksums)
  uint64_t data_bytes;
  // Total number of write operations for data
  uint64_t data_ops;
};

struct DirOptions {
  DirOptions();

  // Total memory reserved for creating memory tables.
  // This includes both the buffer space for writing and sorting tables and the
  // buffer space for creating the data and filter blocks of each table.
  // This, however, does *not* include the buffer space for accumulating
  // writes to ensure an optimized write size.
  // Default: 32MB
  size_t memtable_buffer;

  // Flush memtable as soon as it reaches the specified utilization target.
  // Default: 1 (100%)
  double memtable_util;

  // Estimated average key size.
  // Default: 8 bytes
  size_t key_size;

  // Estimated average value size.
  // Default: 32 bytes
  size_t value_size;

  // Bloom filter bits per key.
  // Set to zero to disable the use of bloom filters.
  // Default: 8 bits
  size_t bf_bits_per_key;

  // Approximate size of user data packed per data block.
  // Note that block is used both as the packaging format and as the logical I/O
  // unit for reading and writing the underlying data log objects.
  // The size of all index and filter blocks are *not* affected
  // by this option.
  // Default: 128K
  size_t block_size;

  // Start zero padding if current estimated block size reaches the
  // specified utilization target. Only applies to data blocks.
  // Default: 0.999 (99.9%)
  double block_util;

  // Set to false to disable the zero padding of data blocks.
  // Only relevant to data blocks.
  // Default: true
  bool block_padding;

  // Number of data blocks to accumulate before flush out to the data log in a
  // single atomic batch. Aggregating data block writes can reduce the I/O
  // contention among multiple concurrent compaction threads that compete
  // for access to a shared data log.
  // Default: 2MB
  size_t block_buffer;

  // Write buffer size for each physical data log.
  // Set to zero to disable buffering and each data block flush
  // becomes an actual physical write to the underlying storage.
  // Default: 4MB
  size_t data_buffer;

  // Write buffer size for each physical index log.
  // Set to zero to disable buffering and each index block flush
  // becomes an actual physical write to the underlying storage.
  // Default: 4MB
  size_t index_buffer;

  // Add necessary padding to the end of each log object to ensure the
  // final object size is always some multiple of the write size.
  // Required by some underlying object stores.
  // Default: false
  bool tail_padding;

  // Thread pool used to run concurrent background compaction jobs.
  // If set to NULL, Env::Default() may be used to schedule jobs if permitted.
  // Otherwise, the caller's thread context will be used directly to serve
  // compactions.
  // Default: NULL
  ThreadPool* compaction_pool;

  // Thread pool used to run concurrent background reads.
  // If set to NULL, Env::Default() may be used to schedule reads if permitted.
  // Otherwise, the caller's thread context will be used directly.
  // Consider setting parallel_reads to true to take full advantage
  // of this thread pool.
  // Default: NULL
  ThreadPool* reader_pool;

  // Set to true to enable parallel reading across different epochs.
  // Otherwise, reads progress serially over all epochs.
  // Default: false
  bool parallel_reads;

  // True if write operations should be performed in a non-blocking manner,
  // in which case a special status is returned instead of blocking the
  // writer to wait for buffer space.
  // Default: false
  bool non_blocking;

  // Number of microseconds to slowdown if a writer cannot make progress
  // because the system has run out of its buffer space.
  // Default: 0
  uint64_t slowdown_micros;

  // If true, the implementation will do aggressive checking of the
  // data it is processing and will stop early if it detects any
  // errors.
  // Default: false
  bool paranoid_checks;

  // True if keys are unique within each epoch.
  // Keys are unique if each key appears no more than once within
  // each epoch.
  // Default: true
  bool unique_keys;

  // Ignore all filters during reads.
  // Default: false
  bool ignore_filters;

  // True if all data read from underlying storage will be verified
  // against the corresponding checksums stored.
  // Default: false
  bool verify_checksums;

  // True if checksum calculation and verification are completely skipped.
  // Default: false
  bool skip_checksums;

  // True if read I/O should be measured.
  // Default: true
  bool measure_reads;

  // True if write I/O should be measured.
  // Default: true
  bool measure_writes;

  // Number of partitions to divide the data. Specified in logarithmic
  // number so each x will give 2**x partitions.
  // REQUIRES: 0 <= lg_parts <= 8
  // Default: 0
  int lg_parts;

  // Env instance used to access objects or files stored in the underlying
  // storage system. If NULL, Env::Default() will be used.
  // Default: NULL
  Env* env;

  // If the env context may be used to run background jobs.
  // Default: false
  bool allow_env_threads;

  // True if the underlying storage is implemented as a parallel
  // file system rather than an object storage.
  // Default: true
  bool is_env_pfs;

  // Rank of the process in the directory.
  // Default: 0
  int rank;
};

// Parse a given configuration string to structured options.
extern DirOptions ParseDirOptions(const char* conf);

// Abstraction for a thread-unsafe and possibly-buffered
// append-only log stream.
class LogSink {
 public:
  LogSink(const std::string& filename, WritableFile* f, port::Mutex* mu = NULL)
      : mu_(mu), filename_(filename), file_(f), offset_(0), refs_(0) {}

  uint64_t Ltell() const {
    if (mu_ != NULL) mu_->AssertHeld();
    return offset_;
  }

  void Lock() {
    if (mu_ != NULL) {
      mu_->Lock();
    }
  }

  Status Lwrite(const Slice& data) {
    if (file_ == NULL) {
      return Status::AssertionFailed("File already closed", filename_);
    } else {
      if (mu_ != NULL) {
        mu_->AssertHeld();
      }
      Status result = file_->Append(data);
      if (result.ok()) {
        result = file_->Flush();
        if (result.ok()) {
          offset_ += data.size();
        }
      }
      return result;
    }
  }

  Status Lsync() {
    Status status;
    if (file_ != NULL) {
      if (mu_ != NULL) mu_->AssertHeld();
      status = file_->Sync();
    }
    return status;
  }

  void Unlock() {
    if (mu_ != NULL) {
      mu_->Unlock();
    }
  }

  Status Lclose(bool sync = false);
  void Ref() { refs_++; }
  void Unref();

 private:
  ~LogSink();
  // No copying allowed
  void operator=(const LogSink&);
  LogSink(const LogSink&);
  Status Close();

  port::Mutex* mu_;  // Constant after construction
  const std::string filename_;
  WritableFile* file_;  // State protected by mu_
  uint64_t offset_;
  uint32_t refs_;
};

// Abstraction for a thread-unsafe and possibly-buffered
// random access log file.
class LogSource {
 public:
  LogSource(RandomAccessFile* f, uint64_t s) : file_(f), size_(s), refs_(0) {}

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) {
    return file_->Read(offset, n, result, scratch);
  }

  uint64_t Size() const { return size_; }

  void Ref() { refs_++; }
  void Unref();

 private:
  ~LogSource();
  // No copying allowed
  void operator=(const LogSource&);
  LogSource(const LogSource&);

  RandomAccessFile* file_;
  uint64_t size_;
  uint32_t refs_;
};

// Destroy the contents of the specified directory.
// Be very careful using this method.
extern Status DestroyDir(const std::string& dirname, const DirOptions& options);

class BatchCursor;

// Deltafs Plfs Dir Writer
class DirWriter {
 public:
  DirWriter() {}
  virtual ~DirWriter();

  // Return the total memory reserved by this directory.
  virtual size_t total_memory_usage() const = 0;
  // Return directory compaction stats for debugging and benchmarking.
  virtual CompactionStats compaction_stats() const = 0;

  // Open an I/O writer against a specified plfs-style directory.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const DirOptions& options, const std::string& dirname,
                     DirWriter** result);

  // Perform a batch of file appends in a single operation.
  // REQUIRES: Finish() has not been called.
  virtual Status Write(BatchCursor* cursor, int epoch = -1) = 0;

  // Append a piece of data to a specific file under the directory.
  // Set epoch to -1 to disable epoch validation.
  // REQUIRES: Finish() has not been called.
  virtual Status Append(const Slice& fid, const Slice& data,
                        int epoch = -1) = 0;

  // Force a memtable compaction.
  // Set epoch to -1 to disable epoch validation.
  // REQUIRES: Finish() has not been called.
  virtual Status Flush(int epoch = -1) = 0;

  // Force a memtable compaction and start a new epoch.
  // Set epoch to -1 to disable epoch validation.
  // REQUIRES: Finish() has not been called.
  virtual Status EpochFlush(int epoch = -1) = 0;

  // Wait for all on-going compactions to finish.
  virtual Status Wait() = 0;

  // Force a compaction and finalize all log files.
  // No further write operation is allowed after this call.
  virtual Status Finish() = 0;

 private:
  // No copying allowed
  void operator=(const DirWriter&);
  DirWriter(const DirWriter&);
};

// Deltafs Plfs Dir Reader
class DirReader {
 public:
  DirReader() {}
  virtual ~DirReader();

  // Open an I/O reader against a specific plfs-style directory.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const DirOptions& options, const std::string& name,
                     DirReader** result);

  // Fetch the entire data from a specific file under a given plfs directory.
  virtual Status ReadAll(const Slice& fid, std::string* dst) = 0;

 private:
  // No copying allowed
  void operator=(const DirReader&);
  DirReader(const DirReader&);
};

}  // namespace plfsio
}  // namespace pdlfs
