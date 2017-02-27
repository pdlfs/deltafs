#pragma once

/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stddef.h>
#include <stdint.h>

#include "pdlfs-common/env.h"

namespace pdlfs {
namespace plfsio {

struct CompactionStats {
  CompactionStats();
  // Total time spent on compaction.
  uint64_t write_micros;

  // Total physical bytes written to the index log.
  uint64_t index_written;
  // Total size of the index log.
  uint64_t index_size;
  // Total physical bytes written to the data log.
  uint64_t data_written;
  // Total size of the data log.
  uint64_t data_size;
};

struct DirOptions {
  DirOptions();

  // Total memory budget for the memory table.
  // Size includes both the raw table and the bloom filter
  // associated with the table.
  // Recommend to allocate 3% of RAM per core for the table.
  // Default: 32MB
  size_t memtable_size;

  // Average size of keys.
  // Default: 8 bytes
  size_t key_size;

  // Average size of values.
  // Default: 32 bytes
  size_t value_size;

  // Bloom filter bits per key.
  // Set to zero to disable the use of bloom filters.
  // Default: 8 bits
  size_t bf_bits_per_key;

  // Approximate size of user data packed per block.
  // Block is used as the packaging format and the basic I/O unit for both
  // reading and writing data logs. The size of index and filter
  // blocks are not affected by this option.
  // Default: 128K
  size_t block_size;

  // Start padding if estimated block size reaches the specified
  // utilization target.
  // Default: 0.999 (99.9%)
  double block_util;

  // Write buffer size for each physical data log.
  // Default: 2MB
  size_t data_buffer;

  // Write buffer size for each physical index log.
  // Default: 2MB
  size_t index_buffer;

  // Add necessary padding to the end of a log to ensure the final size
  // is always some multiple of the write size.
  // Default: false
  bool tail_padding;

  // Thread pool used to run background compaction jobs.
  // If set to NULL, Env::Default() will be used to schedule jobs.
  // Default: NULL
  ThreadPool* compaction_pool;

  // True if write operations should be performed in a non-blocking manner,
  // in which case a special status is returned instead of blocking the
  // writer to wait for buffer space.
  // Default: false
  bool non_blocking;

  // Number of microseconds to slowdown if a writer cannot make progress
  // because the system has run out of its buffer space.
  // Default: 0
  uint64_t slowdown_micros;

  // True if keys are unique within each epoch.
  // Default: true
  bool unique_keys;

  // True if all data read from underlying storage will be
  // verified against corresponding checksums.
  // Default: false
  bool verify_checksums;

  // Number of partitions to divide the data. Specified in logarithmic
  // number so each x will give 2**x partitions.
  // REQUIRES: 0 <= lg_parts <= 8
  // Default: 0
  int lg_parts;

  // Rank of the process.
  // Default: 0
  int rank;

  // Env instance used to access raw files stored in the underlying
  // storage system. If NULL, Env::Default() will be used.
  // Default: NULL
  Env* env;
};

// Parse a given configuration string to structured options.
extern DirOptions ParseDirOptions(const char* conf);

// Abstraction for a thread-unsafe and possibly-buffered
// append-only log stream.
class LogSink {
 public:
  explicit LogSink(WritableFile* f) : file_(f), offset_(0), refs_(0) {}

  uint64_t Ltell() const { return offset_; }

  Status Lwrite(const Slice& data) {
    Status result = file_->Append(data);
    if (result.ok()) {
      result = file_->Flush();
      if (result.ok()) {
        offset_ += data.size();
      }
    }
    return result;
  }

  void Ref() { refs_++; }
  void Unref();

 private:
  enum { kSyncBeforeClosing = true };

  ~LogSink();
  // No copying allowed
  void operator=(const LogSink&);
  LogSink(const LogSink&);

  WritableFile* file_;
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

// Deltafs plfs-style N-1 I/O writer api.
class Writer {
 public:
  Writer() {}
  virtual ~Writer();

  // Return a pointer to dir stats.
  virtual const CompactionStats* stats() const = 0;

  // Open an I/O writer against a specified plfs-style directory.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const DirOptions& options, const std::string& dirname,
                     Writer** result);

  // Append a piece of data to a specified file under a given plfs directory.
  // REQUIRES: Finish() has not been called.
  virtual Status Append(const Slice& fname, const Slice& data) = 0;

  // Sync data to storage.  Not supported so far.
  // REQUIRES: Finish() has not been called.
  virtual Status Sync() = 0;

  // Flush data to make an epoch that benefits future read operations.
  // REQUIRES: Finish() has not been called.
  virtual Status MakeEpoch() = 0;

  // Flush data, finalize indexing, and wait for all outstanding
  // compaction jobs to finish.
  // REQUIRES: Finish() has not been called.
  virtual Status Finish() = 0;

 private:
  // No copying allowed
  void operator=(const Writer&);
  Writer(const Writer&);
};

// Deltafs plfs-style N-1 I/O reader api.
class Reader {
 public:
  Reader() {}
  virtual ~Reader();

  // Open an I/O reader against a specific plfs-style directory.
  // Return OK on success, or a non-OK status on errors.
  static Status Open(const DirOptions& options, const std::string& dirname,
                     Reader** result);

  // List files under a given plfs directory.  Not supported so far.
  virtual void List(std::vector<std::string>* names) = 0;

  // Fetch the entire data from a specific file under a given plfs directory.
  virtual Status ReadAll(const Slice& fname, std::string* dst) = 0;

  // Return true iff a specific file exists under a given plfs directory.
  // Not supported so far.
  virtual bool Exists(const Slice& fname) = 0;

 private:
  // No copying allowed
  void operator=(const Reader&);
  Reader(const Reader&);
};

}  // namespace plfsio
}  // namespace pdlfs
